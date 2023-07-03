# airflow-pipline
to automate the extraction and analysis of trending topics on Twitter and the corresponding top trend videos on YouTube. By leveraging the Twitter API, the project retrieves the top trends for multiple regions. These trends are then used as input for the YouTube Data API to fetch the top three trending videos related to each trend in each region. The project is designed as a data pipeline using Apache Airflow, ensuring the process runs automatically at the end of each day. The resulting insights are consolidated in a report generated and stored in Amazon S3.

Key Features:

    Extracts trending topics from Twitter API for multiple regions.
    Utilizes YouTube Data API to retrieve the top three trending videos for each trend in each region.
    Automates the process using Apache Airflow as a workflow orchestrator.
    Generates a comprehensive report consolidating the trends and top videos.
    Stores the report in Amazon S3 for easy access and scalability.

Technologies Used:

    Python
    Apache Airflow
    Twitter API
    YouTube Data API
    Amazon S3
