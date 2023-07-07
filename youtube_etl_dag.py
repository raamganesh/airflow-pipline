import json
import pendulum
import sys

from datetime import datetime, timedelta

from airflow.decorators import dag, task

import extraction

@dag(   
        default_args={
        "depends_on_past": False,
        "email": ["m.p.raamganesh@gmail.com"],
        "email_on_failure": True,
        "email_on_retry": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        # 'queue': 'bash_queue',
        # 'pool': 'backfill',
        # 'priority_weight': 10,
        # 'end_date': datetime(2016, 1, 1),
        # 'wait_for_downstream': False,
        # 'sla': timedelta(hours=2),
        # 'execution_timeout': timedelta(seconds=300),
        # 'on_failure_callback': some_function, # or list of functions
        # 'on_success_callback': some_other_function, # or list of functions
        # 'on_retry_callback': another_function, # or list of functions
        # 'sla_miss_callback': yet_another_function, # or list of functions
        # 'trigger_rule': 'all_success'
    },
        schedule="0 21 * * *",
        start_date=pendulum.datetime(2023,7,7, tz='UTC'),
        catchup=False,
        tags=["youtube", "test"]
)

def youtube_etl():
    @task
    def extract_data_from_youtube():
        # get_trending_videos function from extraction module to get trending videos from YouTube API
        videos = extraction.get_trending_videos()
        return videos
    
    @task
    def tranform_data( videos ):
        # Transform the extracted video data into a structured DataFrame
        data_frame = extraction.tranform_data( videos )
        return data_frame
    
    @task
    def upload_csv_to_s3( data_frame ):
        # Upload the transformed DataFrame to S3 bucket
        extraction.load_data( data_frame )

    # Workflow tasks
    videos = extract_data_from_youtube()
    data_frame = tranform_data( videos )
    upload_csv_to_s3( data_frame )

youtube_etl()