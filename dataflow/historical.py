import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions
import os
import requests
import logging
import datetime
import json
import re

if __name__ == "__main__":
    os.environ[
        'GOOGLE_APPLICATION_CREDENTIALS'] = r'C:\Users\HP\OneDrive\Desktop\GCP_prac\earthquake-project\keys\all-purpuse-41a4e2945880.json'

    options = PipelineOptions()

    google_cloud_options = options.view_as(GoogleCloudOptions)
    google_cloud_options.project = 'all-purpuse'
    google_cloud_options.job_name = 'historical_data'
    google_cloud_options.region = 'us-central1'
    google_cloud_options.staging_location = 'gs://earthquake-project-dataflow/stagging'
    google_cloud_options.temp_location = 'gs://earthquake-project-dataflow/temp'


    ## read data from request library

    url = r'https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_month.geojson'
    response = requests.get(url)

    if response.status_code == 200:
        content = response.json()  # Get the content of the URL as JSON
        logging.info(f"Data successfully retrieved from {url}")

    else:
        logging.error(f"Failed to retrieve content from {url}. Status code: {response.status_code}")

    content_as_string = json.dumps(content)

    date_str = datetime.datetime.now().strftime('%Y%m%d')
    filename = f"raw_dataflow/{date_str}"
    file_path = f'gs://earthquake-project-main-bucket/{filename}.json'

    # Convert content to JSON string format (as Apache Beam needs string input)
    content_as_string = json.dumps(content)

    # with beam.Pipeline(options=options) as p:
    #     res = (
    #             p
    #             | 'Create PCollection' >> beam.Create([content_as_string])  # Create a PCollection from the content
    #         | 'Write to GCS' >> beam.io.WriteToText(file_path, file_name_suffix='.json', shard_name_template='',num_shards=1)
    #
    #         # Write the content to GCS
    #     )



    def feature_flatten(element):
        content = json.loads(element)
        features = content['features']
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


    def transformation(element):

        for values in element:
            # Safeguard: Check if the time and updated fields have valid epoch values
            try:
                time = float(values['time']) / 1000
                if time > 0:
                    values['time'] = datetime.datetime.utcfromtimestamp(time).strftime('%Y-%m-%d %H:%M:%S')
                else:
                    values['time'] = None  # Set to None or a default value if invalid

                update = float(values['updated']) / 1000
                if update > 0:
                    values['updated'] = datetime.datetime.utcfromtimestamp(update).strftime('%Y-%m-%d %H:%M:%S')
                else:
                    values['updated'] = None  # Set to None or a default value if invalid

            except (ValueError, OSError) as e:
                # Log the error and set default values for invalid timestamps
                print(f"Error converting time: {e}, skipping conversion for this record.")
                values['time'] = None
                values['updated'] = None

            # Extract place name after "of"
            place = values['place']
            pattern = r"of\s+(.+)$"
            match = re.search(pattern, place)
            if match:
                extracted_place = match.group(1)
                values['area'] = extracted_place.strip()

            # Add current timestamp
            insert_date = datetime.datetime.now().timestamp()
            values['insert_date'] = datetime.datetime.utcfromtimestamp(insert_date).strftime('%Y-%m-%d %H:%M:%S')

            yield values

            ## silver layer data add
    date_str = datetime.datetime.now().strftime('%Y%m%d')
    filenames = f"silver_dataflow/{date_str}"
    file_paths = f'gs://earthquake-project-main-bucket/{filenames}'
    read_files = f'gs://earthquake-project-main-bucket/{filenames}.json'


    # with beam.Pipeline(options=options) as p:
    #
    #     res = (
    #             p
    #             | 'Create PCollection' >> beam.io.ReadFromText(file_path)
    #             | 'flattern' >> beam.Map(feature_flatten)
    #             | 'transformation' >> beam.FlatMap(transformation)
    #             | 'write to silver' >> beam.io.WriteToText(file_paths, file_name_suffix='.json', shard_name_template='',num_shards=1)
    #             #| 'print' >> beam.Map(print)
    #     )

    with beam.Pipeline(options=options) as p:

        bq = (
                p
                | 'Read data from GCS' >> beam.io.ReadFromText(read_files)

                | 'write bq' >> beam.io.gcp.bigquery.WriteToBigQuery(
            table='all-purpuse.earthquake_project.earthquake_dataflow',
            # schema = "mag:float,place:string,time:timestamp,updated:timestamp,tz:integer,url:string,detail:string,felt:integer,cdi:integer,mmi:integer,alert:string,status:string,tsunami:integer,sig:integer,net:string,code:string,ids:string,sources:string,types:string,nst:integer,dmin:float,rms:float,gap:integer,magType:string,type:string,title:string,longitude:float,latitude:float,depth:float,area:string,insert_date:timestamp",
            schema='SCHEMA_AUTODETECT',
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
         )
        #     |  'print' >> beam.Map(print)
        )






















