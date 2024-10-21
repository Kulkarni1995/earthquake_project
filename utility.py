################################################################################################
"""
file_name - utility.py
desc - schema define , flattening , and all other transformations
start_date - 2024-10-21

"""


### importing requried modules#
from pyspark.sql import SparkSession
import requests
import os
from google.cloud import storage
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, IntegerType, BooleanType

### Google cloud access key
# Set the environment variable for Google Cloud credentials
os.environ[
    'GOOGLE_APPLICATION_CREDENTIALS'] = r'C:\Users\HP\OneDrive\Desktop\GCP_prac\earthquake-project\keys\all-purpuse-41a4e2945880.json'


### class for reading data from api
class ReadDataFromApiJson:
    def reading(url):

        response = requests.get(url)

        if response.status_code == 200:
            content = response.json()  # Get the content of the URL

            return content
        else:
            print(f"Failed to retrieve content. Status code: {response.status_code}")

# url = r'https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_month.geojson'
#
# features = ReadDataFromApiJson.reading(url)

# print(obj_json)







## class for creating GCS bucket with project name

class CreateCGSBucket:
    def __init__(self, project_name, bucket_name):
        self.project_name = project_name
        self.bucket_name = bucket_name
        self.client = storage.Client(project=self.project_name)

    def createbucket(self, location):
        try:
            # Create a new bucket
            bucket = self.client.bucket(self.bucket_name)
            new_bucket = self.client.create_bucket(bucket, location=location)
            print(f"Bucket {new_bucket.name} created successfully in {location}.")
        except Exception as e:
            print(f"Error creating bucket: {str(e)}")

# project_name = 'all-purpuse'
# bucket_name = 'earthquake-project-main-bucket'
# location = 'us-central1'
# obj = CreateCGSBucket(project_name,bucket_name).createbucket(location)



## upload json file to bucket

# class UploadtoGCSJson():
#     def
