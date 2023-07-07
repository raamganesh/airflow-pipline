# Airflow-pipeline
This project is an ETL (Extract, Transform, Load) pipeline for extracting data from YouTube's trending videos API, transforming the data into a structured format, and loading it into an S3 bucket. The pipeline is built using Apache Airflow, a popular open-source platform for workflow automation and scheduling.

## Key Features:

- Extracts trending video from youtube using youtube data API ( https://developers.google.com/youtube/v3/quickstart/python )
- The YouTube ETL pipeline periodically fetches the most popular trending videos from YouTube's API for multiple regions, including India, the United Kingdom, and the United States. It extracts video metadata such as title, channel, published date, view count, and likes, and transforms it into a structured DataFrame.
- It uploads the transformed data to an S3 bucket for further analysis or storage.
- The process is automated using Apache Airflow as a workflow orchestrator by scheduling the workflow daily at 10 pm.
- Generates a comprehensive report consolidating the key metrics of the trending videos.
- Stores the report in Amazon S3 for easy access and scalability.

## Airflow Dashboard Sanpshot:
![image](https://github.com/raamganesh/airflow-pipline/assets/22257200/1b0afd1a-2e98-4d96-9333-24692d61f5d8)

![image](https://github.com/raamganesh/airflow-pipline/assets/22257200/28d5cb88-5f7e-49e2-ad5a-6c066f48b5aa)

![image](https://github.com/raamganesh/airflow-pipline/assets/22257200/59d37323-047a-4f82-b395-2bbc68711259)


## Future Development:

- Create a historical database using PostgreSQL to store the extracted trend and video data over time, allowing for trend analysis and historical insights.
- Implement the integration with Power BI or Tableau to create an interactive dashboard for visualizing and exploring the trend and video analytics.
- Enhance the data pipeline to include sentiment analysis of tweets and engagement metrics of YouTube videos using the tags extracted from the trending video.
- Set up real-time monitoring and alerting for the data pipeline using tools like Prometheus or Grafana.
- Explore machine learning models to predict upcoming trends based on historical data.

## Technologies Used:

- Python
- Apache Airflow
- Twitter API
- YouTube Data API
- Amazon S3

