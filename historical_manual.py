##########################################################################################
"""
file_name - historical_manual.py
desc - fist time ingection ofone month historical data
start_date - 2024-10-21
"""
##########################################################################################


### importing requried modules
from utility import ReadDataFromApiJson,CreateCGSBucket,UploadtoGCSJson






if __name__ == "__main__":

    ### reading data from api in json format
    ## url for data reading


    url = r'https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_month.geojson'

    content = ReadDataFromApiJson.reading(url)
    #print(type(features))
    ##print(features)

    ##create bucket to store data
    project_name = 'all-purpuse'
    bucket_name = 'earthquake-project-main-bucket'
    location = 'us-central1'
    ## bucket creation
    bucket_obj = CreateCGSBucket(project_name, bucket_name).createbucket(location)

    ## upload json boject to injetion layer


    data_object = content
    destination_blob_prefix = 'landing_data'

    ## json upload object

    json_upload = UploadtoGCSJson.uploadjson(bucket_name,data_object,destination_blob_prefix)

















