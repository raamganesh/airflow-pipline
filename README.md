# Airflow-pipeline

Automate the extraction and analysis of trending topics on Twitter and the corresponding top trend videos on YouTube. By leveraging the Twitter API, the project retrieves the top trends for multiple regions. These trends are then used as input for the YouTube Data API to fetch the top three trending videos related to each trend in each region. The project is designed as a data pipeline using Apache Airflow, ensuring the process runs automatically at the end of each day. The resulting insights are consolidated in a report generated and stored in Amazon S3.

## Key Features:

- Extracts trending topics from Twitter API for multiple regions.
- Utilizes YouTube Data API to retrieve the top three trending videos for each trend in each region.
- Automates the process using Apache Airflow as a workflow orchestrator.
- Generates a comprehensive report consolidating the trends and top videos.
- Stores the report in Amazon S3 for easy access and scalability.

## Future Development:

- Create a historical database using PostgreSQL to store the extracted trend and video data over time, allowing for trend analysis and historical insights.
- Implement the integration with Power BI or Tableau to create an interactive dashboard for visualizing and exploring the trend and video analytics.
- Enhance the data pipeline to include sentiment analysis of tweets and engagement metrics of YouTube videos.
- Set up real-time monitoring and alerting for the data pipeline using tools like Prometheus or Grafana.
- Explore machine learning models to predict upcoming trends based on historical data.

## Technologies Used:

- Python
- Apache Airflow
- Twitter API
- YouTube Data API
- Amazon S3

