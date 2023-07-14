import unittest
from unittest import mock
from extraction import upload_dataframe_to_s3, logger, get_trending_videos, tranform_data
import datetime
import pandas as pd
from pandas.testing import assert_frame_equal


data = [{'Date': datetime.date(2023, 7, 14),
         'region': 'GB',
         'video_ID': 'LyDL9EUIdy0',
         'category_ID': '17',
         'channel_Title': 'The Overlap',
         'title': 'Dele: "Now is the Time to Talk"',
         'published_date': '2023-07-13 07:00:12',
         'rank': 1,
         'tags': ['Sky Sports', 'Sport', 'Football', 'Premier League', 'European Super League', 'Champions League', 'Manchester United', 'Gary Neville', 'MNF', 'Monday Night Football', 'Jamie Carragher', 'the overlap', 'overlap', 'g nev', 'nev', 'overlap interview', 'interview', 'Dele', 'Dele Alli', 'Dele Alli Rehab', 'Dele Alli Child Abuse', 'Dele Alli Sleeping tablets', 'Spurs', 'Everton', 'Sean Dyche', 'Dele Alli and Gary Neville', 'Dele Opens Up', 'USA', 'Besiktas', 'Mauricio Pochettino', 'Dele Alli Drugs', 'Dele Alli Recovery', 'Tottenham', 'England', 'Messi', 'CR7'],
         'number_of_comments': '10346',
         'like_Count': '137376',
         'view_Count': '3891220'}]


raw_video_data = [ {'kind': 'youtube#video', 
                    'etag': '_HZ3-Kojj7kI9h0jLENC9ybvIpY', 
                    'id': 'LyDL9EUIdy0', 
                    'snippet': {
                        'publishedAt': '2023-07-13T07:00:12Z', 
                        'channelId': 'UCjXIw1GlwaY1IzpW_jN9iCQ', 
                        'title': 'Dele: "Now is the Time to Talk"', 
                        'description': ' fake descrition', 
                        'thumbnails': {
                            'default': {
                                'url': 'https://i.ytimg.com/vi/LyDL9EUIdy0/default.jpg', 
                                'width': 120, 
                                'height': 90}, 
                            'medium': {
                                'url': 'https://i.ytimg.com/vi/LyDL9EUIdy0/mqdefault.jpg', 
                                'width': 320, 
                                'height': 180}, 
                            'high': {
                                'url': 'https://i.ytimg.com/vi/LyDL9EUIdy0/hqdefault.jpg', 
                                'width': 480, 
                                'height': 360}, 
                            'standard': {
                                'url': 'https://i.ytimg.com/vi/LyDL9EUIdy0/sddefault.jpg', 
                                'width': 640, 
                                'height': 480}, 
                            'maxres': {
                                'url': 'https://i.ytimg.com/vi/LyDL9EUIdy0/maxresdefault.jpg', 
                                'width': 1280, 
                                'height': 720}}, 
                        'channelTitle': 'The Overlap', 
                        'tags': ['Sky Sports', 'Tottenham', 'England', 'Messi', 'CR7'], 
                        'categoryId': '17', 
                        'liveBroadcastContent': 'none', 
                        'localized': {
                            'title': 'Dele: "Now is the Time to Talk"', 
                            'description': 'fake desc'}, 
                        'defaultAudioLanguage': 'en-GB'}, 
                        'statistics': {
                            'viewCount': '3918109', 
                            'likeCount': '137925', 
                            'favoriteCount': '0', 
                            'commentCount': '10380'}, 
                            'region': 'GB', 
                            'rank': 1}]

expected_df_dict = [{'Date': datetime.date(2023, 7, 14), 
                     'region': 'GB', 
                     'video_ID': 'LyDL9EUIdy0', 
                     'category_ID': '17', 
                     'channel_Title': 'The Overlap', 
                     'title': 'Dele: "Now is the Time to Talk"', 
                     'published_date': datetime.datetime.strptime('2023-07-13 07:00:12', '%Y-%m-%d %H:%M:%S'), 
                     'rank': 1, 
                     'tags': ['Sky Sports', 'Tottenham', 'England', 'Messi', 'CR7'], 
                     'number_of_comments': '10380', 
                     'like_Count': '137925', 
                     'view_Count': '3918109'}]

expected_dataframe = pd.DataFrame(expected_df_dict)

class ExtractionTestCase(unittest.TestCase):

    # sample DataFrame
    dataframe = pd.DataFrame(data)

    def test_get_trending_videos_success(self):
        # Test case for successful API request
        videos = get_trending_videos()

        # Test the returned value is not None
        self.assertIsNotNone(videos)

        # Test list instance
        self.assertIsInstance(videos, list)

    def test_tranform_data(self):
        # Test case for transforming videos into a DataFrame
        data_frame = tranform_data(raw_video_data)

        # Testing the dataframe is not None
        self.assertIsNotNone(data_frame)

        # Testing the dataframe instance
        self.assertIsInstance(data_frame, pd.DataFrame)

        # Testing the dataframe values
        assert_frame_equal(expected_dataframe, data_frame)

    def test_upload_dataframe_to_s3_success(self):
        bucket_name = "my-bucket"
        key = "data.csv"

        # Mock the S3 client and its put_object method
        with mock.patch('extraction.boto3.Session') as mock_session:
            mock_s3 = mock_session.return_value.client.return_value
            mock_s3.put_object.return_value = {'ResponseMetadata': {'HTTPStatusCode': 200}}

            with self.assertLogs(logger, level='INFO') as log:
                upload_dataframe_to_s3(self.dataframe, bucket_name, key)

        self.assertTrue(any(f"File '{key}' uploaded successfully to S3 bucket '{bucket_name}'" in msg for msg in log.output))
        mock_session.assert_called_once_with(region_name='us-west-2', aws_access_key_id=mock.ANY, aws_secret_access_key=mock.ANY)
        mock_s3.put_object.assert_called_once_with(Body=mock.ANY, Bucket=bucket_name, Key=key)

    def test_upload_dataframe_to_s3_failure(self):
        # Test case for failed upload to S3
        # Simulate an error by providing an invalid bucket name or key
        bucket_name = "invalid-bucket"
        key = "data.csv"

        # Mock the S3 client and its put_object method to raise an exception
        with mock.patch('extraction.boto3.Session') as mock_session:
            mock_s3 = mock_session.return_value.client.return_value
            mock_s3.put_object.side_effect = Exception("Failed to upload")

            with self.assertLogs(logger, level='ERROR') as log:
                upload_dataframe_to_s3(self.dataframe, bucket_name, key)

        self.assertTrue(any(f"Failed to upload file '{key}' to S3 bucket '{bucket_name}'" in msg for msg in log.output))
        mock_session.assert_called_once_with(region_name='us-west-2', aws_access_key_id=mock.ANY, aws_secret_access_key=mock.ANY)
        mock_s3.put_object.assert_called_once_with(Body=mock.ANY, Bucket=bucket_name, Key=key)

if __name__ == '__main__':
    unittest.main()