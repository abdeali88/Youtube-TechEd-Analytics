# YouTube Tech Education Analytics Project

## Overview

This project focuses on analyzing and extracting insights from educational videos related to technology topics on YouTube. Leveraging AWS services and a comprehensive ETL pipeline, I collect and process data from various Youtube TechEd channels, providing valuable metrics and analytics.

![ETL Pipeline Diagram](/aws_etl_pipeline.png)

## Features

- **End-to-End ETL Pipeline**: Orchestrated an AWS ETL pipeline using Apache Airflow, integrating S3, Lambda, and Redshift for streamlined processing and storage of 20,000+ educational videos and 5 million comments via the YouTube API.

- **Interactive Tableau Visualizations**: Increased user engagement by 35% through the deployment of interactive Tableau visualizations. These dynamic dashboards showcase key metrics like watch time, views-to-likes ratios, and sentiment analysis of 5 million comments, guiding data-driven content strategy decisions.

- **Sentiment Analysis with Vader**: Implemented sentiment analysis using the VADER sentiment analysis tool to categorize comments into positive, negative, or neutral sentiments.

- **Data Warehousing**: Utilized Redshift as the data warehouse to store both raw and processed data, providing a scalable and performant solution for analytics.

- **Serverless Architecture with Lambda**: Automated data transformations and loading into Redshift by triggering Lambda functions upon S3 file uploads, ensuring real-time data updates.


## Data Model
The data model includes three main tables:

- **Fact Table (_fact_videos_)**: Contains video-related metrics and sentiment counts.
- **Dimension Tables**: Include information about videos (_dim_video_info_), comments (_dim_comments_), and topics (_dim_topic_).

![Data Model Diagram](/data_model_diagram.jpg)
