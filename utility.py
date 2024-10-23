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
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, IntegerType, BooleanType,FloatType
from datetime import datetime
import json
import pyspark.sql.functions as f
import pandas as pd
from io import StringIO
import logging
from google.cloud import bigquery


# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')



### Google cloud access key
# Set the environment variable for Google Cloud credentials
os.environ[
    'GOOGLE_APPLICATION_CREDENTIALS'] = r'C:\Users\HP\OneDrive\Desktop\GCP_prac\earthquake-project\keys\all-purpuse-41a4e2945880.json'


### class for reading data from api
class ReadDataFromApiJson:
    @staticmethod
    def reading(url):
        try:
            response = requests.get(url)

            if response.status_code == 200:
                content = response.json()  # Get the content of the URL
                logging.info(f"Data successfully retrieved from {url}")
                return content
            else:
                logging.error(f"Failed to retrieve content from {url}. Status code: {response.status_code}")
                return None
        except requests.exceptions.RequestException as e:
            logging.error(f"Error occurred while retrieving data from {url}: {e}")
            raise




class CreateCGSBucket:
    def __init__(self, project_name, bucket_name):
        self.project_name = project_name
        self.bucket_name = bucket_name
        try:
            self.client = storage.Client(project=self.project_name)
            logging.info(f"Google Cloud Storage client initialized for project: {self.project_name}")
        except Exception as e:
            logging.error(f"Error initializing Google Cloud Storage client: {e}")
            raise

    def createbucket(self, location):
        try:
            # Create a new bucket
            bucket = self.client.bucket(self.bucket_name)
            new_bucket = self.client.create_bucket(bucket, location=location)
            logging.info(f"Bucket {new_bucket.name} created successfully in {location}.")
        except Exception as e:
            logging.error(f"Error creating bucket: {e}")
            raise


class UploadtoGCS:
    @staticmethod
    def uploadjson(bucket_name, data_object, destination_blob_prefix):
        """Uploads a JSON object to a GCS bucket."""
        try:
            # Initialize GCS client and bucket
            storage_client = storage.Client()
            bucket = storage_client.bucket(bucket_name)

            # Create a unique file name with the current date
            date_str = datetime.now().strftime('%Y%m%d')
            filename = f"{date_str}.json"

            # Specify the blob (path) within the bucket where the file will be stored
            destination_blob_name = f"{destination_blob_prefix}/{filename}"
            blob = bucket.blob(destination_blob_name)

            # Upload the JSON data directly from memory to GCS
            blob.upload_from_string(data=json.dumps(data_object), content_type='application/json')

            logging.info(f"Upload of {filename} to {destination_blob_name} complete.")
        except Exception as e:
            logging.error(f"Error uploading JSON to GCS: {e}")
            raise

    @staticmethod
    def upload_parquet(bucket_name, dataframe, destination_blob_prefix):
        """Uploads a DataFrame as a Parquet file to a GCS bucket."""
        try:
            # Create a unique file name with the current date
            date_str = datetime.now().strftime('%Y%m%d')

            # Define the local path for the Parquet file
            local_temp_file = f"C:\\Users\\HP\\OneDrive\\Desktop\\GCP_prac\\earthquake-project\\silver_data\\{date_str}.parquet"

            # Ensure the directory exists
            os.makedirs(os.path.dirname(local_temp_file), exist_ok=True)

            # Repartition the DataFrame to a single partition and write to local Parquet file
            dataframe = dataframe.repartition(1)
            dataframe.write.parquet(local_temp_file, mode="overwrite")

            # Initialize GCS client and specify the destination blob path
            storage_client = storage.Client()
            destination_blob_name = f"{destination_blob_prefix}/{date_str}.parquet"
            bucket = storage_client.bucket(bucket_name)
            blob = bucket.blob(destination_blob_name)

            # Upload the local Parquet file to GCS
            blob.upload_from_filename(local_temp_file)

            # Optionally, delete the temporary local file after upload
            os.remove(local_temp_file)

            logging.info(f"Upload of {destination_blob_name} to GCS complete.")
        except Exception as e:
            logging.error(f"Error uploading Parquet file to GCS: {e}")
            raise

    @staticmethod
    def upload_json(bucket_name, dataframe, destination_blob_prefix):
        """Uploads a PySpark DataFrame as a JSON file directly to a GCS bucket."""
        try:
            # Create a unique file name with the current date
            date_str = datetime.now().strftime('%Y%m%d')
            filename = f"{date_str}.json"

            # Convert DataFrame rows to JSON format
            json_rdd = dataframe.toJSON()

            # Collect the JSON data from the RDD to a list of strings
            json_data_list = json_rdd.collect()

            # Join the list of JSON strings into one large JSON string
            json_data = "\n".join(json_data_list)

            # Initialize GCS client
            storage_client = storage.Client()
            bucket = storage_client.bucket(bucket_name)

            # Specify the destination blob name
            destination_blob_name = f"{destination_blob_prefix}/{filename}"
            blob = bucket.blob(destination_blob_name)

            # Upload the JSON data from the string to GCS
            blob.upload_from_string(json_data, content_type='application/json')

            logging.info(f"Upload of {filename} to {destination_blob_name} complete.")
        except Exception as e:
            logging.error(f"Error uploading JSON DataFrame to GCS: {e}")
            raise




class GCSFileHandler:
    def __init__(self, bucket_name):
        """Initialize the class with bucket information."""
        self.storage_client = storage.Client()  # Uses the environment variable for credentials
        self.bucket_name = bucket_name

    def download_json_as_text(self, json_file_name):
        """Download a JSON file from GCS and return it as a Python dictionary."""
        try:
            # Access the GCS bucket
            bucket = self.storage_client.bucket(self.bucket_name)

            # Retrieve the JSON file from the bucket
            blob = bucket.blob(json_file_name)

            # Download the JSON data as a text string
            json_data = blob.download_as_text()

            # Convert the JSON string to a Python dictionary
            json_dict = json.loads(json_data)

            logging.info(f"Successfully downloaded {json_file_name} from GCS.")

            # Return the JSON data as a dictionary
            return json_dict
        except Exception as e:
            logging.error(f"Error downloading {json_file_name} from GCS: {e}")
            raise


class EarthquakeDataFrameCreation:
    def __init__(self, json_data):
        self.json_data = json_data
        self.spark = SparkSession.builder.appName('Earthquake Data Processor').getOrCreate()

        # Set up logging
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
        self.logger = logging.getLogger(__name__)

    def process_data(self):
        try:
            features = self.json_data['features']
            self.logger.info('Processing %d features', len(features))

            data = []
            for value in features:
                properties = value['properties']

                # Extract geometry coordinates
                geometry = {
                    'longitude': value['geometry']['coordinates'][0],
                    'latitude': value['geometry']['coordinates'][1],
                    'depth': value['geometry']['coordinates'][2]
                }

                # Update properties with geometry
                properties.update(geometry)
                data.append(properties)

            return data
        except KeyError as e:
            self.logger.error('KeyError: %s', e)
            raise
        except Exception as e:
            self.logger.error('An error occurred while processing data: %s', e)
            raise

    def convert_to_dataframe(self):
        try:
            data = self.process_data()

            schema = StructType([
                StructField("mag", FloatType(), True),
                StructField("place", StringType(), True),
                StructField("time", StringType(), True),
                StructField("updated", StringType(), True),
                StructField("tz", StringType(), True),
                StructField("url", StringType(), True),
                StructField("detail", StringType(), True),
                StructField("felt", IntegerType(), True),
                StructField("cdi", FloatType(), True),
                StructField("mmi", FloatType(), True),
                StructField("alert", StringType(), True),
                StructField("status", StringType(), True),
                StructField("tsunami", IntegerType(), True),
                StructField("sig", IntegerType(), True),
                StructField("net", StringType(), True),
                StructField("code", StringType(), True),
                StructField("ids", StringType(), True),
                StructField("sources", StringType(), True),
                StructField("types", StringType(), True),
                StructField("nst", IntegerType(), True),
                StructField("dmin", FloatType(), True),
                StructField("rms", FloatType(), True),
                StructField("gap", FloatType(), True),
                StructField("magType", StringType(), True),
                StructField("type", StringType(), True),
                StructField("title", StringType(), True),
                StructField("longitude", FloatType(), True),
                StructField("latitude", FloatType(), True),
                StructField("depth", FloatType(), True)
            ])

            processed_data = []
            for entry in data:
                processed_entry = {}
                for key, value in entry.items():
                    # Convert fields to float where necessary
                    if key in ['mag', 'cdi', 'mmi', 'dmin', 'rms', 'gap', 'depth']:
                        processed_entry[key] = float(value) if value is not None else None
                    else:
                        processed_entry[key] = value
                processed_data.append(processed_entry)

            # Create Spark DataFrame
            df = self.spark.createDataFrame(processed_data, schema)
            self.logger.info('DataFrame created successfully with %d records', df.count())
            return df
        except Exception as e:
            self.logger.error('An error occurred while converting data to DataFrame: %s', e)
            raise

class Transformation:

    def process(df):
        try:
            # Transformations
            df_transformed = df \
                .withColumn('of_position', f.instr(f.col('place'), 'of')) \
                .withColumn('length', f.length(f.col('place'))) \
                .withColumn('area', f.expr("substring(place, of_position + 3, length - of_position - 2)")) \
                .withColumn('time', f.from_unixtime(f.col('time') / 1000)) \
                .withColumn('updated', f.from_unixtime(f.col('updated') / 1000)) \
                .withColumn('injection_date', f.current_timestamp()) \
                .drop('of_position', 'length')

            return df_transformed
        except Exception as e:
            raise RuntimeError(f'An error occurred during DataFrame transformation: {e}')
class UploadToBigquery:
    def __init__(self, project_name, dataset_name):
        self.project_id = project_name
        self.dataset_id = dataset_name

    def to_bigquery(self, table_name, df, write_mode="overwrite"):
        try:
            df.write \
                .format("bigquery") \
                .option("table", f"{self.project_id}:{self.dataset_id}.{table_name}") \
                .mode(write_mode) \
                .save()

            print(f"Data written to BigQuery table {self.dataset_id}.{table_name}")
        except Exception as e:
            raise RuntimeError(f"An error occurred while writing to BigQuery: {e}")




class ManualUploadtoBigQuery:
    def load_json_from_gcs_to_bigquery(bucket_name, file_path, dataset_id, table_id, project_id):
        """
        Loads a JSON file from GCS into a BigQuery table, creating the dataset and table if they don't exist.

        Parameters:
        bucket_name (str): Name of the GCS bucket.
        file_path (str): Path to the JSON file inside the GCS bucket.
        dataset_id (str): BigQuery dataset ID.
        table_id (str): BigQuery table ID.
        project_id (str): GCP project ID.

        Raises:
        Exception: If the load job fails or dataset creation fails.
        """



        schema = [
            {"name": "mag", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "place", "type": "STRING", "mode": "NULLABLE"},
            {"name": "time", "type": "STRING", "mode": "NULLABLE"},
            {"name": "updated", "type": "STRING", "mode": "NULLABLE"},
            {"name": "tz", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "url", "type": "STRING", "mode": "NULLABLE"},
            {"name": "detail", "type": "STRING", "mode": "NULLABLE"},
            {"name": "felt", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "cdi", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "mmi", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "alert", "type": "STRING", "mode": "NULLABLE"},
            {"name": "status", "type": "STRING", "mode": "NULLABLE"},
            {"name": "tsunami", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "sig", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "net", "type": "STRING", "mode": "NULLABLE"},
            {"name": "code", "type": "STRING", "mode": "NULLABLE"},
            {"name": "ids", "type": "STRING", "mode": "NULLABLE"},
            {"name": "sources", "type": "STRING", "mode": "NULLABLE"},
            {"name": "types", "type": "STRING", "mode": "NULLABLE"},
            {"name": "nst", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "dmin", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "rms", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "gap", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "magType", "type": "STRING", "mode": "NULLABLE"},
            {"name": "type", "type": "STRING", "mode": "NULLABLE"},
            {"name": "title", "type": "STRING", "mode": "NULLABLE"},
            {"name": "longitude", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "latitude", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "depth", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "area", "type": "STRING", "mode": "NULLABLE"},
            {"name": "injection_date", "type": "STRING", "mode": "NULLABLE"}
        ]

        try:
            # Initialize BigQuery client with the given project ID
            client = bigquery.Client(project=project_id)

            # Define the full table reference
            table_ref = f"{project_id}.{dataset_id}.{table_id}"

            # Check if the dataset exists
            dataset_ref = client.dataset(dataset_id)
            try:
                client.get_dataset(dataset_ref)  # Make an API call to check if the dataset exists
                print(f"Dataset '{dataset_id}' already exists.")
            except Exception:
                # Create the dataset if it does not exist
                dataset = bigquery.Dataset(dataset_ref)
                dataset.location = "US"  # You can set the location as needed
                client.create_dataset(dataset)
                print(f"Created dataset '{dataset_id}'.")

            # Define the GCS URI to the JSON file
            uri = f"gs://{bucket_name}/{file_path}"

            # Configure the load job to create a table and detect the schema
            job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
                schema=schema,  # Automatically detects the schema
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,  # Overwrites if table exists
            )

            # Start the load job
            print(f"Starting load job from {uri} to {table_ref}...")
            load_job = client.load_table_from_uri(uri, table_ref, job_config=job_config)

            # Wait for the load job to complete
            load_job.result()  # Waits for the job to finish

            print(f"Successfully loaded JSON data from {uri} into {table_ref}.")

            # Get table details and print the number of rows loaded
            table = client.get_table(table_ref)
            print(f"Loaded {table.num_rows} rows into {table_ref}.")

        except Exception as e:
            print(f"An error occurred: {e}")





