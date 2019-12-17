import os
import pandas as pd
import pyarrow
import pyproj
import configparser
from crash_utilities import *
from datetime import datetime

#read the config file to extract the aws credentials
config = configparser.ConfigParser()
config.read('config_file.cfg')
os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']

#set the base s3_path for subsequent manipulation 
s3_path = config['S3']['S3_BUCKET_PATH']


def wa_vehicle_summary_harmoniser(df):
    """
    Function which maps vehicle type fields to the vehicle types preferred in the dataset.
    ---
    
    Keyword Arguments:
    df -- pandas dataframe object which contains the summary unit information for the NZ dataset. 
    
    Returns:
    df -- Pandas dataframe object which contains the information harmonised to the preferred type list. 
    """
    logging.info('Parsing vehicle data...')
    
    logging.info('Declare mappings')
    #declare mappings
    animal_fields = []
    car_sedan_fields = []
    car_utility_fields = []
    car_van_fields = []
    car_4x4_fields = []
    car_station_wagon_fields = []
    motor_cycle_fields = ['TOTAL_MOTOR_CYCLE_INVOLVED']
    truck_small_fields = ['TOTAL_TRUCK_INVOLVED']
    truck_large_fields = ['TOTAL_HEAVY_TRUCK_INVOLVED'] 
    bus_fields = []
    taxi_fields = []
    bicycle_fields = ['TOTAL_BIKE_INVOLVED']
    scooter_fields = []
    pedestrian_fields = ['TOTAL_PEDESTRIANS_INVOLVED']
    inanimate_fields = []
    train_fields = []
    tram_fields = []
    vehicle_other_fields = ['TOTAL_OTHER_VEHICLES_INVOLVED']
    
    
    
    
    
    
    
    

    
    
    logging.info('Summing values...')
    #apply the mapping by summing columns in each field name. 
    #df['animals'] = df[animal_fields].astype(int).apply(sum, axis=1).fillna(0)
    #df['car_sedan'] = df[car_sedan_fields].astype(int).apply(sum, axis=1).fillna(0)
    #df['car_utility'] = df[car_utility_fields].astype(int).apply(sum, axis=1).fillna(0)
    #df['car_van'] = df[car_van_fields].apply(sum, axis=1).fillna(0)
    #df['car_4x4'] = df[car_4x4_fields].astype(int).apply(sum, axis=1).fillna(0)
    #df['car_station_wagon'] = df[car_station_wagon_fields].apply(sum, axis=1).fillna(0)
    df['motor_cycle'] = df[motor_cycle_fields].astype(int).apply(sum, axis=1).fillna(0)
    df['truck_small'] = df[truck_small_fields].astype(int).apply(sum, axis=1).fillna(0)
    df['truck_large'] = df[truck_large_fields].astype(int).apply(sum, axis=1).fillna(0)
    #df['bus'] = df[bus_fields].astype(int).apply(sum, axis=1).fillna(0)
    #df['taxi'] = df[taxi_fields].astype(int).apply(sum, axis=1).fillna(0)
    df['bicycle'] = df[bicycle_fields].astype(int).apply(sum, axis=1).fillna(0)
    #df['scooter'] = df[scooter_fields].astype(int).apply(sum, axis=1).fillna(0)
    df['pedestrian'] = df[pedestrian_fields].astype(int).apply(sum, axis=1).fillna(0)
    #df['inanimate'] = df[inanimate_fields].astype(int).apply(sum, axis=1).fillna(0)
    #df['train'] = df[train_fields].astype(int).apply(sum, axis=1).fillna(0)
    #df['tram'] = df[tram_fields].apply(sum, axis=1).fillna(0)
    df['vehicle_other'] = df[vehicle_other_fields].astype(int).apply(sum, axis=1).fillna(0)
    
    #add in two missing fields using structure checker
    logging.info('Adding missing fields...')
    df = structure_checker(df, expected_vehicle_fields, coerce=True, add_only = True)
    
    #add in 0s
    df[expected_vehicle_fields] = df[expected_vehicle_fields].fillna(0).astype(int)
    
    #generate IDs 
    df['vehicles_id'] = df.apply(vehicles_id_generator, axis=1)
            
    return df
    
def wa_description_harmoniser(df):
    """
    Function which accepts an nz dataset pandas dataframe and returns it with field descriptions harmonised.
    
    Keyword Arguments:
    df - dataframe object containing the nz formatted description data.
    
    Returns:
    df - dataframe object with reformatted description information."""
    logging.info('Harmonising description data...')
    
    #go down the list of things we can get for vic
    #severity - coerce to int bcause victoria records as int and dictionary accepts string keys
    df['severity'] = df['SEVERITY'].apply(lambda x: severity_dict[x])
    
    #build the crash type field out of EVENT_NATURE and EVENT_TYPE
    df['crash_type'] = df['EVENT_NATURE'] + ' ' +  df['EVENT_TYPE']

    #intersection and midblock are opposites of one another.
    df['midblock'] = (df['ACCIDENT_TYPE'].fillna('Unknown').apply(lambda x: midblock_dict[x]))
    df['intersection'] = (df['midblock']==False)
    
    return df

def wa_datetime_handler(df):
    """
    Function which transforms the datetime data in the nz dataset.
    ---
    
    Keyword Arguments:
    df - dataframe object containing the nz formatted datetime data.
    
    Returns:
    df - dataframe object with reformatted datetime information."""
    logging.info('Parsing datetime data...')
    
    #first reformat the float type provided by pandas as an integer, filling 0 - also get the last four characters after prepending 0s 
    df['crash_time_mod'] = ('000'+df['CRASH_TIME'].fillna(0).astype(int).astype(str)).str[-4:]
   
    #join the date and the time - where you don't have a time, fill 0. 
    df['CRASH_DATETIME'] = df['CRASH_DATE'] + ' ' + df['crash_time_mod']
    
    #cast to datetime
    df['datetime'] = df['CRASH_DATETIME'].str.strip().apply(lambda x: datetime.strptime(x, '%d/%m/%Y %H%M'))
    
    
    #extract relevant parts
    df['year'] = df['datetime'].dt.year
    df['month'] = df['datetime'].dt.month
    df['day_of_week'] = (df['datetime'].dt.dayofweek+1)
    df['hour'] = df['datetime'].dt.hour
    df['day_of_month'] = df['datetime'].dt.day
    
    #assign approximate false because information provided is sufficient.
    df = df.assign(approximate = False)
    
    #generate id using string operations.
    df['date_time_id'] = (df['year'].apply(str) + '-' + df['month'].apply(str) + '-' + df['day_of_month'].fillna('').apply(str) + '-' + df['day_of_week'].apply(str) 
                         + '-' + df['hour'].apply(str))
    return df
    
def wa_location_handler(df):
    """
    Function which transforms the location data in the nz dataset.
    ---
    
    Keyword Arguments:
    df - dataframe object containing the nz formatted location data.
    
    Returns:
    df - dataframe object with reformatted location information. 
    """
    logging.info('Parsing location data...')
    
    #generate the key field
    df['lat_long'] = df[['LATITUDE', 'LONGITUDE']].apply(lambda x: (x[0], x[1]), axis=1)
    
    #assign country and state
    df = df.assign(country = 'AU')
    df = df.assign(state = 'WA')
        
    #rename fields
    df = df.rename(columns = {'LONGITUDE': 'latitude', 'LONGITUDE': 'longitude'})
    
    return df
    
    
def wa_main():
    """
    Function which conducts the etl to the final staging structure for new zealand data
    ---
    
    Keyword Arguments:
    None.
    
    Returns:
    Data frame containing the structure coerced appropriately to the correct format.
    """
    #read the datasets
    logging.info('Read in datasets...')
    wa_df = pd.read_parquet(s3_path + '/crash_wa/crash')
    
    #field harmonisation
    logging.info('Harmonising and renaming fields...')
    wa_df = wa_vehicle_summary_harmoniser(wa_df)
    wa_df = wa_description_harmoniser(wa_df)

    #field name changing
    wa_df = wa_datetime_handler(wa_df)
    wa_df = wa_location_handler(wa_df)
    
    #generate the primary key field.
    logging.info('Generating IDs...')
    wa_df['crash_id'] = wa_df['ACC_ID'].astype(str).apply(lambda x: 'WA'+x)
    logging.info('Coercing to final staging structure...')
    wa_df = structure_checker(wa_df, expected_fields, coerce = True)
    
    return wa_df


if __name__ is '__main__':
    wa_main()