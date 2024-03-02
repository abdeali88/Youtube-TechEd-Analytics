import json
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import boto3
import psycopg2

def get_sentiment(comment):
    analyzer = SentimentIntensityAnalyzer()
    sentiment_scores = analyzer.polarity_scores(comment)

    if sentiment_scores['compound'] >= 0.05:
        return 'Positive'
    elif sentiment_scores['compound'] <= -0.05:
        return 'Negative'
    else:
        return 'Neutral'

def lambda_handler(event, context):
    # Extract relevant information from the S3 event
    s3_bucket = event['Records'][0]['s3']['bucket']['name']
    s3_key = event['Records'][0]['s3']['object']['key']

    # Initialize S3 and Redshift clients
    s3 = boto3.client('s3')
    redshift = psycopg2.connect(
        #Removed for security purposes
        dbname='',
        user='',
        password='',
        host='',
        port=''
    )

    # Download JSON file from S3
    response = s3.get_object(Bucket=s3_bucket, Key=s3_key)
    video_data = json.load(response['Body'])

    # Extract information from JSON
    topic = video_data['topic']
    video_id = video_data['videoId']
    comments = video_data['comments']

    # Calculate sentiment counts
    sentiment_results = [get_sentiment(comment) for comment in comments]
    positive_comments = sentiment_results.count('Positive')
    negative_comments = sentiment_results.count('Negative')
    neutral_comments = sentiment_results.count('Neutral')

    # Initialize SentimentIntensityAnalyzer
    analyzer = SentimentIntensityAnalyzer()

    # Load data into Redshift
    with redshift.cursor() as cursor:
        # Get the topic_id for the current topic
        cursor.execute(f"SELECT topic_id FROM dim_topic WHERE topic = '{topic}';")
        topic_id = cursor.fetchone()[0]

        # Insert into 'fact_videos' table
        insert_query = f"""
            INSERT INTO fact_videos (
                video_id, topic_id, title, published_at, positive_comment_count, negative_comment_count, neutral_comment_count
            )
            VALUES (
                {video_id}, {topic_id}, '{video_data['snippet']['title']}', '{video_data['snippet']['publishedAt']}',
                {positive_comments}, {negative_comments}, {neutral_comments}
            );
        """
        cursor.execute(insert_query)

        # Insert into 'dim_video_info' table
        insert_query = f"""
            INSERT INTO dim_video_info (
                video_id, topic, description, channel_title, channel_id, tags, default_language, default_audio_language,
                duration, dimension, definition, caption, licensed_content, projection,
                positive_comment_count, negative_comment_count, neutral_comment_count
            )
            VALUES (
                {video_id}, '{topic}', '{video_data['snippet']['description']}', '{video_data['snippet']['channelTitle']}',
                '{video_data['snippet']['channelId']}', ARRAY{video_data['snippet']['tags']},
                '{video_data['snippet']['defaultLanguage']}', '{video_data['snippet']['defaultAudioLanguage']}',
                '{video_data['contentDetails']['duration']}', '{video_data['contentDetails']['dimension']}',
                '{video_data['contentDetails']['definition']}', '{video_data['contentDetails']['caption']}',
                {video_data['contentDetails']['licensedContent']}, '{video_data['contentDetails']['projection']}',
                {positive_comments}, {negative_comments}, {neutral_comments}
            );
        """
        cursor.execute(insert_query)

        # Insert into 'dim_comments' table
        for idx, comment_text in enumerate(comments):
            sentiment = sentiment_results[idx]
            sentiment_score = analyzer.polarity_scores(comment_text)['compound']
            insert_query = f"""
                INSERT INTO dim_comments (comment_id, video_id, text, sentiment, sentiment_score)
                VALUES ({idx + 1}, {video_id}, '{comment_text}', '{sentiment}', {sentiment_score});
            """
            cursor.execute(insert_query)

    # Commit and close the Redshift connection
    redshift.commit()
    redshift.close()

    return {
        'statusCode': 200,
        'body': json.dumps('Sentiment analysis results loaded to Redshift successfully.')
    }
