import json
from googleapiclient.errors import HttpError
import googleapiclient.discovery
import os
import boto3


#AWS S3 Intitalize
s3 = boto3.client('s3', aws_access_key_id='', aws_secret_access_key='')
s3_bucket_name = 'youtube-tech-ed-analytics'

#  YouTube API credentials
os.environ["OAUTHLIB_INSECURE_TRANSPORT"] = "1"
api_service_name = "youtube"
api_version = "v3"
DEVELOPER_KEY = "" #Removed for security purpose


# Function to fetch all comments
def fetch_all_comments(video_id, youtube):
    all_comments = []

    # Check if comments are disabled for the video
    try:
    # Initial request to get the first page of comments
        comments_response = youtube.commentThreads().list(
            part="snippet",
            videoId=video_id,
            maxResults=100
        ).execute()

    except HttpError as e:
        if e.resp.status == 403 and "commentsDisabled" in str(e):
            print(f"Comments are disabled for the video with ID {video_id}. Skipping.")
            return all_comments


    # Keep fetching comments until there are no more pages
    while comments_response:
        # Extract comments from the current page
        comments = [item["snippet"]["topLevelComment"]["snippet"]["textDisplay"] for item in comments_response.get("items", [])]
        all_comments.extend(comments)

        # Check if there are more pages
        if "nextPageToken" in comments_response:
            # Make the next request with the nextPageToken
            comments_response = youtube.commentThreads().list(
                part="snippet",
                videoId=video_id,
                maxResults=100,
                pageToken=comments_response["nextPageToken"]
            ).execute()
        else:
            # No more pages, break out of the loop
            break

    return all_comments

# Function to fetch video data
def fetch_video_data(topic, **kwargs):
    ti = kwargs['ti']
    youtube = googleapiclient.discovery.build(api_service_name, api_version, developerKey=DEVELOPER_KEY)

    try:
        with open(f"resume_{topic}.txt", "r") as resume_file:
            next_page = resume_file.read().strip()
            next_page_token = next_page if next_page else None

        count = 0
        # Loop to get 500 video Id's
        while count < 500:

            with open(topic+".txt", "r") as id_file:
                existing_ids = id_file.read().splitlines()
                count = len(existing_ids)

            search_response = youtube.search().list(
                q=topic,
                type="video",
                part="id",
                maxResults=50,
                pageToken=next_page_token
            ).execute()

            video_ids = [item["id"]["videoId"] for item in search_response.get("items", [])]

            for video_id in video_ids:

                with open(topic+".txt", "r") as id_file:
                    existing_ids = id_file.read().splitlines()
                    if video_id in existing_ids:
                        print(f"Video ID {video_id} already processed. Skipping.")
                        continue

                # Get video details using videos().list() endpoint
                video_response = youtube.videos().list(
                    id=video_id,
                    part="snippet, contentDetails, statistics"
                ).execute()

                all_comments = fetch_all_comments(video_id, youtube)

                 # Extract relevant information from the responses
                video_details = video_response.get("items", [])[0]

                video_snippet = video_details.get("snippet", {})

                #extrct only desired fields from snippet
                desired_fields = ["title", "publishedAt", "description", "channelTitle", "channelId", "tags", "defaultLanguage", "defaultAudioLanguage"]
                video_snippet = {key: video_snippet.get(key, None) for key in desired_fields}

                video_content_details = video_details.get("contentDetails", {})
                video_statistics = video_details.get("statistics", {})
                
                videoInfo = {"topic": topic, "videoId": video_id, "snippet":video_snippet, "contentDetails":video_content_details, "statistics":video_statistics, "comments":all_comments}

                # Append video_id to the file only after the JSON file is created
                with open(f"{topic}.txt", "a") as id_file:
                    id_file.write(video_id + "\n")

                #WRITE FILE TO s3 bucket
                s3_file_key = f"videos/{topic}/{video_id}.json"
                try:
                    # Upload the videoInfo JSON to S3
                    s3.put_object(Body=json.dumps(videoInfo), Bucket=s3_bucket_name, Key=s3_file_key)                    
                    print(f"File uploaded to S3: {s3_file_key}")
                    print("Credentials not available")
                
                except Exception as e:
                    print(f"Error during S3 upload: {str(e)}")

                count += 1

            # Introduce a delay of 10 minutes (600 seconds) before the next iteration
            ti.sleep(600)

            next_page_token = search_response.get("nextPageToken")
            if not next_page_token:
                break

    except Exception as e:
        print("Exception" + str(e))
        state = next_page_token
        with open(f"resume_{topic}.txt", "w") as state_file:
            state_file.write(state)
