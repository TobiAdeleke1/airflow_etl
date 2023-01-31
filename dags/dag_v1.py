import requests
import os
import pandas as pd
import pendulum


from airflow.decorators import dag, task


now = pendulum.now()

BASE_DIR =  os.path.dirname(__file__)
DATASET_DIR = f'{BASE_DIR}/datasets'
os.makedirs(DATASET_DIR,exist_ok=True)

@dag(dag_id='houseprice_etl2',start_date=now, schedule="@once", catchup=False)
def houseprices_etl2():
    @task(task_id='uk_houseprices_1')
    def download_uk_houseprices():
        print("Downloading UK houseprices file ...")
    
        uk_url= "http://prod.publicdata.landregistry.gov.uk.s3-website-eu-west-1.amazonaws.com/pp-complete.csv"

        
        uk_download= requests.get(uk_url)  
        with open(f'{DATASET_DIR}/uk_house_prices.csv','wb') as c:
            c.write(uk_download.content)
    
        return f"{DATASET_DIR}/uk_house_prices.csv"

    @task(task_id='uk_postcode_2')
    def download_postcode_data():
      
        print("Downloading UK postcode dataset ...")
        POSTCODE_DIR= f'{DATASET_DIR}/postcode'
        os.makedirs(POSTCODE_DIR,exist_ok=True)
        postcode_url ='https://data.freemaptools.com/download/full-uk-postcodes/ukpostcodes.zip'  
        
        
        postcode_download= requests.get(postcode_url)  

        with open(f"{POSTCODE_DIR}/postcode.zip",'wb') as c:
            c.write(postcode_download.content)
            c.close()
    
        return f"{POSTCODE_DIR}/postcode.zip"

    @task(task_id='import_csv_data')
    def import_csv(datasets_path):
        """
        Importing a csv file
        """
        print(f"imporing file in")
        print(f"imporing file in: {datasets_path}")
        csv_file = pd.read_csv(datasets_path) 
        return csv_file

    @task(task_id='ten_year_data_breakdown') 
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
    
    @task(task_id='generate_postcode_dict') 
    def get_postcode_dict(uk_postcode_df):
        """
        
        """
        print("Generating Postcode dict ")
        postcode_dict= dict(zip(uk_postcode_df['postcode'],zip(uk_postcode_df['latitude'],uk_postcode_df['longitude'])))

        return postcode_dict
    

    @task(task_id='merge_houseprices_postcode') 
    def get_merged_postcode_houseprices(postcode_dict,houseprices_df):
        """
        
        """

        print("Merging Postcode data to House Prices data ..")

        def get_lat_long(input_postcode):  
            try:
                lat, long = postcode_dict[input_postcode]
            except:
                    # print("input_postcode", input_postcode)
                    lat,long = '',''    
            return lat,long

        latitude_longitude =houseprices_df['postcode'].apply(lambda x : get_lat_long(x))
        houseprices_df['latitude']= [x for x, _ in latitude_longitude]
        houseprices_df['longitude']= [y for _, y in latitude_longitude]

        houseprices_df.to_csv(f"{DATASET_DIR}/houseprices_10years.csv")
        return f"{DATASET_DIR}/houseprices_10years.csv"
    
    @task(task_id='get_final_property_types') 
    def get_property_type_csv(postcode_houseprices_df):

        print("Splitting the House prices based on Data type")
        """
        # Split data based on property type:
        D = Detached, S = Semi-Detached, T = Terraced, 
        F = Flats/Maisonettes, O = Other
        """
        property_type_name= ['terraced','flats','detached','semi_detached','other']
        property_type=list(postcode_houseprices_df['property_type'].unique())
        property_type_list= list(zip(property_type_name,property_type))

        for p_name, p_type in property_type_list:
            property_name = f"houseprices_{p_name}"
            property_csv= postcode_houseprices_df[postcode_houseprices_df['property_type']==p_type]
            property_csv.to_csv(f"{DATASET_DIR}/{property_name}.csv")


    # #1. Download uk prices csv --> works
    csv_path = download_uk_houseprices()
    # #2.Get the whole file
    uk_prices_df= import_csv(csv_path)
    # #3. Clean uk price data --> works
    new_uk_path = get_10years_houseprice(uk_prices_df)
    new_10year_prices_df= import_csv(new_uk_path)
    #4. Get postcode data
    uk_postcode_path= download_postcode_data()
    uk_postcode_df= import_csv(uk_postcode_path)
    uk_postcode_dict= get_postcode_dict(uk_postcode_df)

    #5. Merge postcode data with housesprices
    postcode_houseprices_path= get_merged_postcode_houseprices(uk_postcode_dict,new_10year_prices_df)
    postcode_houseprices_df= import_csv(postcode_houseprices_path)

    #6. Create files based on property type, to build models based on different types
    get_property_type_csv(postcode_houseprices_df)

houseprices_etl2()