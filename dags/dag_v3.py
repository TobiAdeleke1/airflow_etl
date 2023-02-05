from tasks import (download_uk_houseprices,
                   postcode_data,
                   get_merged_postcode_houseprices,
                   get_property_type_csv,
                   make_database_connection)

import os
import json
import pendulum
from airflow.decorators import dag, task

now = pendulum.now()

url = 'http://prod.publicdata.landregistry.gov.uk.s3-website-eu-west-1.amazonaws.com/pp-monthly-update-new-version.csv'
postcode_url ='https://data.freemaptools.com/download/full-uk-postcodes/ukpostcodes.zip'  
BASE_DIR =  os.path.dirname(__file__)
DATASET_DIR = f'{BASE_DIR}/datasets'
os.makedirs(DATASET_DIR,exist_ok=True)

@dag(dag_id='houseprice_etl3',start_date=now, schedule="@once", catchup=False)
def houseprices_etl3():

    @task(task_id='uk_houseprices_31')
    def retrieve():
        csv_path = download_uk_houseprices(url)
        return csv_path
    
    @task(task_id='uk_postcode_3')
    def retrieve_postcode():
        postcode_dict = postcode_data(postcode_url)
        out_dict = json.dumps(postcode_dict)

        dict_path = f"{DATASET_DIR}/dict.json"
        with open(dict_path, 'w') as outfile:
            outfile.write(out_dict)
        # return postcode_dict
        return dict_path

    @task(task_id='combine_postcode_ukprices')
    def clean_data(uk_postcode_dict, houseprice_path):
     
        clean_data_path = get_merged_postcode_houseprices(uk_postcode_dict,houseprice_path)
        return clean_data_path
    
    @task(task_id='split_combined_postcode_prices')
    def transform_data(datapath):
        file_json = get_property_type_csv(datapath)
        return file_json

      
    @task(task_id='send_to_database') 
    def send_to_database(files_dict):
        final_clean = make_database_connection(files_dict)
        return 'DONE'


    


    #TODO: NEED TO CONSIDER WHAT ARE DEPEND AND SETTING THAT UP, just Debugging left and using airflow logging tool
    # import postcode data and get dict
    uk_postcode_path = retrieve_postcode()
    # Download uk prices csv --> works
    houseprices_path = retrieve()
    # # Get the whole file 
    uk_prices_path = clean_data( uk_postcode_path,houseprices_path)

    # # final version
    filename_dict= transform_data(uk_prices_path)

    # # send file to database
    # send_to_database(filename_dict)

    

  



houseprices_etl3()