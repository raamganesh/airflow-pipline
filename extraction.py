import json
import requests
import config
import pandas as pd
import datetime
import boto3
import os


import logging

# Set up logging configuration
# Create a directory in the script's location
log_directory = os.path.join(os.path.dirname(os.path.abspath(__file__)), "log")
os.makedirs(log_directory, exist_ok=True)

log_file_path = os.path.join(log_directory, f"logfile_{datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.log")

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(filename=log_file_path),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger()

# Function to make API request
def make_api_request(url, params):
    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        return json.loads(response.content)["items"]
    except requests.exceptions.RequestException as e:
        logger.error(f"Error making API request: {e}")
        return None
    except (json.JSONDecodeError, KeyError) as e:
        logger.error(f"Error parsing API response: {e}")
        return None

def get_trending_videos():
    """Gets the list of trending videos for the given regions."""

    api_key = config.API_KEY
    regions = [ "IN", "GB", "US" ]
    videos = []

    for region in regions:
        url = "https://www.googleapis.com/youtube/v3/videos"
        params = {
            "part": "snippet, statistics",
            "regionCode": region,
            "type": "video",
            "chart": "mostPopular",
            "maxResults": 20,
            "key": api_key
        }

        # Make API request to get trending videos
        data = make_api_request(url, params)
        if data is not None:
            # Process and format the video data by adding the region and rank information 
            data = [{**video, "region": region, "rank": rank + 1} for rank, video in enumerate(data)]
            videos.extend(data)

    return videos

def tranform_data( videos ):
    """Extract the the required infromation from the meta data"""
    video_list = []
    for video in videos:
        video_dict = dict()
        video_dict['region'] = video.get('region')
        video_dict['video_ID'] = video.get('id')
        video_dict['category_ID'] = video['snippet'].get('categoryId')
        video_dict['channel_Title'] = video['snippet'].get('channelTitle')
        video_dict['title'] = video['snippet']['localized'].get('title')
        video_dict['published_date'] = datetime.datetime.strptime(video['snippet'].get('publishedAt'), "%Y-%m-%dT%H:%M:%SZ") if video['snippet'].get('publishedAt') else None
        video_dict['rank'] = video.get('rank')
        video_dict['tags'] = video['snippet'].get('tags')
        video_dict['number_of_comments'] = video['statistics'].get('commentCount')
        video_dict['like_Count'] = video['statistics'].get('likeCount')
        video_dict['view_Count'] = video['statistics'].get('viewCount')
    
        video_list.append(video_dict)

    # Add todays date as a column in the dataframe
    today = datetime.date.today()
    video_df = pd.DataFrame( video_list )

    video_df.insert(0, 'Date', today)

    return video_df


def upload_dataframe_to_s3(dataframe, bucket_name, key):
    # Convert DataFrame to CSV string
    csv_buffer = dataframe.to_csv(index=False)

    # create a boto3 session
    session = boto3.Session(
        region_name='us-west-2',
        aws_access_key_id=config.aws_access_key,
        aws_secret_access_key=config.aws_secret_key
    )

    s3 = session.client('s3')

    # Upload CSV data directly to S3 bucket
    # Encode CSV string as bytes
    response = s3.put_object(
        Body=csv_buffer.encode('utf-8'),
        Bucket=bucket_name,
        Key=key
    )

    if response['ResponseMetadata']['HTTPStatusCode'] == 200:
        logger.info(f"File '{key}' uploaded successfully to S3 bucket '{bucket_name}'")
    else:
        logger.error(f"Failed to upload file '{key}' to S3 bucket '{bucket_name}'")

def load_data( df ):
    "define the bucket name and filename and upload the data to s3"
    bucket_name = "youtube-etl-airflow"
    filename = f"youtube_{datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.csv"

    # Upload the DataFrame to S3 bucket
    upload_dataframe_to_s3(df, bucket_name, filename)