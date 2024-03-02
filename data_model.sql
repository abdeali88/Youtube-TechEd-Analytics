-- Create Fact Table
CREATE TABLE fact_videos (
    video_id INT PRIMARY KEY REFERENCES dim_video_info(video_id),
    topic_id INT REFERENCES dim_topic(topic_id),
    title VARCHAR(255),
    published_at TIMESTAMP,
    view_count INT,
    like_count INT,
    favorite_count INT,
    comment_count INT,
    positive_comment_count INT,
    negative_comment_count INT,
    neutral_comment_count INT
);

-- Create Dimension Tables
CREATE TABLE dim_video_info (
    video_id INT PRIMARY KEY,
    topic VARCHAR(255),
    description TEXT,
    channel_title VARCHAR(255),
    channel_id VARCHAR(50),
    tags VARCHAR(255) [], -- Assuming tags are stored as an array
    default_language VARCHAR(10),
    default_audio_language VARCHAR(10),
    duration VARCHAR(20),
    dimension VARCHAR(20),
    definition VARCHAR(20),
    caption VARCHAR(20),
    licensed_content BOOLEAN,
    projection VARCHAR(20),
    positive_comment_count INT,
    negative_comment_count INT,
    neutral_comment_count INT
);

CREATE TABLE dim_comments (
    comment_id INT PRIMARY KEY,
    video_id INT REFERENCES dim_video_info(video_id),
    text TEXT,
    sentiment VARCHAR(10),
    sentiment_score FLOAT
);

CREATE TABLE dim_topic (
    topic_id INT PRIMARY KEY,
    topic VARCHAR(255)
);
