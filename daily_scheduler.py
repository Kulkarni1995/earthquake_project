########################################################################
"""
file_name - daily_schedular.py
desc - schema define , flattening , and all other transformations
start_date - 2024-10-23

"""

from utility import ReadDataFromApiJson, UploadtoGCS, GCSFileHandler, EarthquakeDataFrameCreation, Transformation, UploadToBigquery, CreateCGSBucket,ManualUploadtoBigQuery,DailyUploadtoBigQuery
from datetime import datetime
from pyspark.sql import SparkSession



if __name__ == '__main__':

    try:
        ### Reading data from API in JSON format
        url = r'https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_day.geojson'
        content = ReadDataFromApiJson.reading(url)


        ### jsonfile to bucket
        # project_name = 'all-purpuse'
        bucket_name = 'earthquake-project-main-bucket'
        # location = 'us-central1'

        ## ### Upload JSON object to injection layer

        data_object = content
        destination_blob_prefix = 'landing_data'
        json_upload = UploadtoGCS.uploadjsondaily(bucket_name, data_object, destination_blob_prefix)

        # Create a unique file name with the current date
        date_str = datetime.now().strftime('%Y%m%d')
        filename = f"daily{date_str}.json"

        ### Read data from GCS landing location
        gcs_handler = GCSFileHandler(bucket_name)
        json_file_name = f"landing_data/{filename}"
        json_data = gcs_handler.download_json_as_text(json_file_name)

        processor = EarthquakeDataFrameCreation(json_data)  ##
        df = processor.convert_to_dataframe()

        # ### Transformation on DataFrame
        df = Transformation.process(df)

        ### Load to GCS bucket
        destination_blob_prefix = 'silver_data'
        silver_data = UploadtoGCS.upload_json_daily(bucket_name, df, destination_blob_prefix)


        ##  # ### Load to BigQuery
        project_id = 'all-purpuse'
        dataset_id = 'earthquake_project'
        table_id = 'earthquake_table'  # Corrected typo
        file_path = f'silver_data/daily{date_str}.json'


        bq = DailyUploadtoBigQuery.load_json_from_gcs_to_bigquery(bucket_name,file_path,dataset_id,table_id,project_id)

        print("Data ingestion and upload completed successfully.")

    except Exception as e:
        print(f"An error occurred: {e}")





