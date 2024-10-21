##########################################################################################
"""
file_name - historical_manual.py
desc - fist time ingection ofone month historical data
start_date - 2024-10-21
"""
##########################################################################################


### importing requried modules
from utility import (ReadDataFromApiJson,CreateCGSBucket)






if __name__ == "__main__":

    ### reading data from api in json format
    ## url for data reading


    url = r'https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_month.geojson'

    content = ReadDataFromApiJson.reading(url)
    #print(type(features))
    ##print(features)






    #create bucket to store data
    # project_name = 'all-purpuse'
    # bucket_name = 'earthquake-project-main-bucket'
    # location = 'us-central1'
    # obj = CreateCGSBucket(project_name, bucket_name).createbucket(location)













