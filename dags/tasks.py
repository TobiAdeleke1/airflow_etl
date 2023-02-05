import requests
import os
import json
import pandas as pd
import psycopg2



BASE_DIR =  os.path.dirname(__file__)
DATASET_DIR = f'{BASE_DIR}/datasets'
os.makedirs(DATASET_DIR,exist_ok=True)

def download_uk_houseprices(uk_url):

    uk_download= requests.get(uk_url)  
    with open(f'{DATASET_DIR}/uk_house_prices.csv','wb') as c:
        c.write(uk_download.content)

    return f"{DATASET_DIR}/uk_house_prices.csv"

def import_csv(datasets_path):
    
        print(f"imporing file in: {datasets_path}")
        csv_file = pd.read_csv(datasets_path) 
        return csv_file

def get_postcode_dict(uk_postcode_df):
        print("Generating Postcode dict ")
        postcode_dict= dict(zip(uk_postcode_df['postcode'],zip(uk_postcode_df['latitude'],uk_postcode_df['longitude'])))
        return postcode_dict

def postcode_data(postcode_url):
      
        print("Downloading UK postcode dataset ...")
        POSTCODE_DIR= f'{DATASET_DIR}/postcode'
        os.makedirs(POSTCODE_DIR,exist_ok=True)
        # postcode_url ='https://data.freemaptools.com/download/full-uk-postcodes/ukpostcodes.zip'  
        
        
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
    print(f"Processing UK houses price, to only use past 10 years")
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
  
    print("Merging Postcode data to House Prices data ..")

    houseprices_v1df = import_csv(houseprices_csv)
    # clean the 10 years
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
    

        print("Splitting the House prices based on Data type")
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

def db_connection(file_name, table_name):
     # hERE THE PUrpose it to make connection to the posgreasql database
      #---> 1. install pip install psycopg2-binary : https://www.psycopg.org/docs/
      # --> 2. TO create query to postgrql: need a connection object and cursor object, so a function to copy from csv
    #   conn = psycopg2.connect(database='airflow',
    #                           host= 'localhost',
    #                           user= 'airflow',
    #                           password='airflow',
    #                           port = '5432' )
     
        conn = psycopg2.connect(database='airflow',
                                user= 'airflow',
                                password='airflow',
                                port = '5432' )
        try:
        
            cursor = conn.cursor()
            # for csv file sep=';'
            cursor.copy_from(file_name, table_name, sep=';')

            #commit changes to the database persistent
            conn.commit()
        
        except psycopg2.Error as e:
            print('Error:', e)
        
        finally:
            #close connection to the database
            cursor.close()
            conn.close()

def make_database_connection(filename_dict):
    for tablename,filename in filename_dict.item():
        #store csv to databse
        db_connection(filename,tablename)


    
      
 
    # iterate through the produced files and dump them into the database
    pass