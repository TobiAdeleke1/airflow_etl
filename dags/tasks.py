import requests
import os
import json
import pandas as pd
import psycopg2
from sqlalchemy import create_engine


BASE_DIR =  os.path.dirname(__file__)
DATASET_DIR = f'{BASE_DIR}/datasets'
os.makedirs(DATASET_DIR,exist_ok=True)

url = 'http://prod.publicdata.landregistry.gov.uk.s3-website-eu-west-1.amazonaws.com/pp-monthly-update-new-version.csv'
postcode_url ='https://data.freemaptools.com/download/full-uk-postcodes/ukpostcodes.zip' 
 

def download_uk_houseprices(uk_url):

    uk_download= requests.get(uk_url)  
    with open(f'{DATASET_DIR}/uk_house_prices.csv','wb') as c:
        c.write(uk_download.content)

    return f"{DATASET_DIR}/uk_house_prices.csv"

def import_csv(datasets_path):
        csv_file = pd.read_csv(datasets_path) 
        return csv_file

def get_postcode_dict(uk_postcode_df):
        postcode_dict= dict(zip(uk_postcode_df['postcode'],zip(uk_postcode_df['latitude'],uk_postcode_df['longitude'])))
        return postcode_dict

def postcode_data(postcode_url):
        POSTCODE_DIR= f'{DATASET_DIR}/postcode'
        os.makedirs(POSTCODE_DIR,exist_ok=True)    
        postcode_download= requests.get(postcode_url)  

        with open(f"{POSTCODE_DIR}/postcode.zip",'wb') as c:
            c.write(postcode_download.content)
            c.close()
        
        datapath = f"{POSTCODE_DIR}/postcode.zip"
        csv_df = import_csv(datapath)
        postcode_dict = get_postcode_dict(csv_df)

        return postcode_dict

def get_10years_houseprice(uk_houseprice_df):
    """
    
    """
    #random column name generation
    name_list= [f"name_{num}" for num in range(0,len(uk_houseprice_df.columns))]
    #reset columns
    uk_houseprice_df.columns=name_list

    uk_houseprice_df['year']= uk_houseprice_df['name_2'].apply(lambda x:x.split(" ")[0])
    uk_houseprice_df['year'] = pd.to_datetime(uk_houseprice_df['year'])
    house_price_10years= uk_houseprice_df[uk_houseprice_df['year'].dt.year > 2012]

    # house_price_sub= house_price_10years.drop(['Unnamed: 0','name_7','name_8','name_10','name_15'],axis=1)
    house_price_sub= house_price_10years.drop(['name_7','name_8','name_10','name_15'],axis=1)
    specfic_column =['unique_id','price','date_of_transfer','postcode','property_type','old_or_new','duration','address','city','district','county','ppd_category','year']
    house_price_sub.columns= specfic_column
    house_price_sub.to_csv(f"{DATASET_DIR}/houseprices_10years.csv")

    return f"{DATASET_DIR}/houseprices_10years.csv"

def get_merged_postcode_houseprices(postcode_json,houseprices_csv):
  
    houseprices_v1df = import_csv(houseprices_csv)
    houseprices_path = get_10years_houseprice(houseprices_v1df)
    houseprices_df = import_csv(houseprices_path)


    with open(postcode_json,'r') as json_file:
        postcode_dict = json.load(json_file)

    def get_lat_long(input_postcode):  
        try:
            lat, long = postcode_dict[input_postcode]
        except:
                # print("input_postcode", input_postcode)
                lat,long = '',''    
        return lat,long

    latitude_longitude = houseprices_df['postcode'].apply(lambda x : get_lat_long(x))
    houseprices_df['latitude'] = [x for x, _ in latitude_longitude]
    houseprices_df['longitude']= [y for _, y in latitude_longitude]

    houseprices_df.to_csv(f"{DATASET_DIR}/houseprices_10years.csv")
    return f"{DATASET_DIR}/houseprices_10years.csv"

def get_property_type_csv(postcode_houseprices_path):
    
        """
        # Split data based on property type:
        D = Detached, S = Semi-Detached, T = Terraced, 
        F = Flats/Maisonettes, O = Other
        """
        postcode_houseprices_df =  import_csv(postcode_houseprices_path)
        property_type_name= ['terraced','flats','detached','semi_detached','other']
        property_type=list(postcode_houseprices_df['property_type'].unique())
        property_type_list= list(zip(property_type_name,property_type))
        
        # output dict
        filenames_dict = {}
        for p_name, p_type in property_type_list:
            property_name = f"houseprices_{p_name}"
            property_csv= postcode_houseprices_df[postcode_houseprices_df['property_type']==p_type]
            filepath_name = f"{DATASET_DIR}/{property_name}.csv"
            filenames_dict[property_name] = filepath_name
            
            property_csv.to_csv(filepath_name)
        
        output_dict = json.dumps(filenames_dict)
        dict_path = f"{DATASET_DIR}/property_dict.json"

        with open(dict_path, 'w') as outfile:
            outfile.write(output_dict)

        return dict_path

def db_connection( create_query):
        # connecting to a database i created locally in pgADMIN called 'airflow_data'
        conn = psycopg2.connect(database='airflow_data',
                                user='postgres',
                                host='host.docker.internal',
                                password='learn2DATA2',
                                port = '5432' )
        try:
            cursor = conn.cursor()
            cursor.execute(create_query)
            conn.commit()
        
        except psycopg2.Error as e:
            print('Error:', e)
        
        finally:
            #close connection to the database
            cursor.close()
            conn.close()

def sqlalchemy_db_connection(data, table_name):
     # Drop tables 
     drop_query = f'''DROP TABLE if exists {table_name}'''
     db_connection(drop_query)
     

     create_query = f'''
                    CREATE TABLE {table_name} (
                    index INT PRIMARY KEY,
                    unique_id CHAR(50),
                    price numeric,
                    date_of_transfer TIMESTAMP,
                    postcode CHAR(10),
                    property_type CHAR(20),
                    old_or_new CHAR(10),
                    duration CHAR(10),
                    address CHAR(255),
                    city CHAR(100),
                    district CHAR(255),
                    county CHAR(255),
                    ppd_category CHAR(255),
                    year CHAR(20),
                    latitude float8,
                    longitude float8
                    )
                    '''
    
    # create table with pscopy
     db_connection(create_query) 

    # sql alchemy to copy df to created table 
     user = "postgres"
     database = "airflow_data"
     pass_word = "learn2DATA2"
     host = "host.docker.internal"
     connection_string = f"postgresql://{user}:{pass_word}@{host}/{database}"
     sql_db = create_engine(connection_string)
     sql_con = sql_db.connect()


     # data to sql 
     data.to_sql(table_name, sql_con, if_exists='replace')
   
def make_database_connection():
     
    CSVFILES = [csv_file for csv_file in os.listdir(DATASET_DIR) if csv_file.endswith('.csv') \
                and not (csv_file.endswith('10years.csv') or csv_file.endswith('prices.csv') )]
 
    for csv_file in CSVFILES:
        tablename = csv_file.split('.')[0]
        csv_filepath = f"{DATASET_DIR}/{csv_file}"    
        csv_df = pd.read_csv(csv_filepath)
        df_columnlist = [str(col) for col in list(csv_df.columns) if not col.startswith('Unnamed')]
        clean_df = csv_df[df_columnlist]
        sqlalchemy_db_connection(clean_df,tablename)

