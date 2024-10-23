##########################################################################################
"""
file_name - historical_manual.py
desc - first-time injection of one month historical data
start_date - 2024-10-21
"""
##########################################################################################

### Importing required modules
from utility import ReadDataFromApiJson, UploadtoGCS, GCSFileHandler, EarthquakeDataFrameCreation, Transformation, UploadToBigquery, CreateCGSBucket,ManualUploadtoBigQuery
from datetime import datetime
import os


if __name__ == "__main__":
    try:
        ### Reading data from API in JSON format
        url = r'https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_month.geojson'
        content = ReadDataFromApiJson.reading(url)

        ### Create bucket to store data
        project_name = 'all-purpuse'
        bucket_name = 'earthquake-project-main-bucket'
        location = 'us-central1'

        # Check if the bucket already exists or create it
        bucket_obj = CreateCGSBucket(project_name, bucket_name).createbucket(location)

        ### Upload JSON object to injection layer
        data_object = content
        destination_blob_prefix = 'landing_data'
        json_upload = UploadtoGCS.uploadjson(bucket_name, data_object, destination_blob_prefix)

        # Create a unique file name with the current date
        date_str = datetime.now().strftime('%Y%m%d')
        filename = f"{date_str}.json"

        ### Read data from GCS landing location
        gcs_handler = GCSFileHandler(bucket_name)
        json_file_name = f"landing_data/{filename}"
        json_data = gcs_handler.download_json_as_text(json_file_name)

        ### Creating DataFrame
        processor = EarthquakeDataFrameCreation(json_data)  ##
        df = processor.convert_to_dataframe()

        # ### Transformation on DataFrame
        df = Transformation.process(df)
        #
        # print(df.show(truncate=False))

        ### Load to GCS bucket
        destination_blob_prefix = 'silver_data'
        silver_data = UploadtoGCS.upload_json(bucket_name, df, destination_blob_prefix)
        #
        # ### Load to BigQuery
        project_id = 'all-purpuse'
        dataset_id = 'earthquake_project'
        table_id = 'earthquake_table'  # Corrected typo
        file_path = f'silver_data/{date_str}.json'

        bq  = ManualUploadtoBigQuery.load_json_from_gcs_to_bigquery(bucket_name,file_path,dataset_id,table_id,project_id)

        print("Data ingestion and upload completed successfully.")

    except Exception as e:
        print(f"An error occurred: {e}")























