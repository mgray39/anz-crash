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


def nz_vehicle_summary_harmoniser(df):
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
    animal_fields = ['animals']
    car_sedan_fields = ['carStation']
    car_utility_fields = ['vanOrUtili']
    car_van_fields = []
    car_4x4_fields = ['suv']
    car_station_wagon_fields = []
    motor_cycle_fields = ['motorcycle']
    truck_small_fields = []
    truck_large_fields = ['truck'] 
    bus_fields = ['bus', 'schoolBus']
    taxi_fields = ['taxi']
    bicycle_fields = ['bicycle']
    scooter_fields = ['moped']
    pedestrian_fields = ['Pedestrian']
    inanimate_fields = ['bridge', 'cliffBank', 'debris', 'ditch', 'fence', 'guardRail', 'houseBuild', 'kerb', 'objectThro', 'overBank', 'parkedVehi', 'phoneBoxEt',
                        'postOrPole', 'roadworks', 'slipFlood', 'strayAnima', 'trafficIsl', 'trafficSig', 'tree', 'waterRiver']
    train_fields = ['train']
    tram_fields = []
    vehicle_other_fields = ['otherVehic', 'unknownVeh', 'other', 'vehicle']
    
    logging.info('Summing values...')
    #apply the mapping by summing columns in each field name. 
    df['animals'] = df[animal_fields].astype(int).apply(sum, axis=1).fillna(0)
    df['car_sedan'] = df[car_sedan_fields].astype(int).apply(sum, axis=1).fillna(0)
    df['car_utility'] = df[car_utility_fields].astype(int).apply(sum, axis=1).fillna(0)
    #df['car_van'] = df[car_van_fields].apply(sum, axis=1).fillna(0)
    df['car_4x4'] = df[car_4x4_fields].astype(int).apply(sum, axis=1).fillna(0)
    #df['car_station_wagon'] = df[car_station_wagon_fields].apply(sum, axis=1).fillna(0)
    df['motor_cycle'] = df[motor_cycle_fields].astype(int).apply(sum, axis=1).fillna(0)
    #df['truck_small'] = df[truck_small_fields].apply(sum, axis=1).fillna(0)
    df['truck_large'] = df[truck_large_fields].astype(int).apply(sum, axis=1).fillna(0)
    df['bus'] = df[bus_fields].astype(int).apply(sum, axis=1).fillna(0)
    df['taxi'] = df[taxi_fields].astype(int).apply(sum, axis=1).fillna(0)
    df['bicycle'] = df[bicycle_fields].astype(int).apply(sum, axis=1).fillna(0)
    df['scooter'] = df[scooter_fields].astype(int).apply(sum, axis=1).fillna(0)
    df['pedestrian'] = df[pedestrian_fields].astype(int).apply(sum, axis=1).fillna(0)
    df['inanimate'] = df[inanimate_fields].astype(int).apply(sum, axis=1).fillna(0)
    df['train'] = df[train_fields].astype(int).apply(sum, axis=1).fillna(0)
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
    
def nz_casualties_summary_harmoniser(df):
    """
    Function which maps casualty type fields to the casualty types preferred in the dataset.
    ---
    
    Keyword Arguments:
    df -- pandas dataframe object which contains the casualty information for the nz dataset. 
    
    Returns:
    df -- Pandas dataframe object which contains the information harmonised to the preferred type list. 
    """
    logging.info('Harmonising casualty data...')
    #first rename some columns
    df = df.rename(columns={'fatalCount':'fatalities', 'seriousInj':'serious_injuries', 'minorInjur':'minor_injuries'})
    
    #get the casualties field
    df['casualties'] = df['fatalities'] + df['serious_injuries'] + df['minor_injuries']
    
    #now generate the id field
    df['casualties_id'] = df.apply(casualties_id_generator, axis=1)
    
    return df
    
def nz_description_harmoniser(df):
    """
    Function which accepts an nz dataset pandas dataframe and returns it with field descriptions harmonised.
    
    Keyword Arguments:
    df - dataframe object containing the nz formatted description data.
    
    Returns:
    df - dataframe object with reformatted description information."""
    logging.info('Harmonising description data...')
    
    #go down the list of things we can get for vic
    #severity - coerce to int bcause victoria records as int and dictionary accepts string keys
    df['severity'] = df['crashSever'].astype(str).apply(lambda x: severity_dict[x])
    
    #intersection and midblock are opposites of one another.
    df['midblock'] = df['intersec_1'].apply(lambda x: midblock_dict[x])
    df['intersection'] = (df['midblock']==False)
    
    #road positions
    df['road_position_horizontal'] = df['roadCurvat'].apply(lambda x: road_position_horizontal_dict[x])
    df['road_position_vertical'] = df['flatHill'].apply(lambda x: road_position_vertical_dict[x])
    
    #other condition
    df['weather'] = df['weatherA'].apply(lambda x: weather_dict[x])
    df['lighting'] = df['light'].apply(lambda x: lighting_dict[x])
    df['traffic_controls'] = df['trafficCon'].fillna('Nil').apply(lambda x: traffic_controls_dict[x])
    
    #rename: speedLimit --> speed_limit
    df = df.rename(columns = {'speedLimit': 'speed_limit'})
    
    return df

def nz_datetime_handler(df):
    """
    Function which transforms the datetime data in the nz dataset.
    ---
    
    Keyword Arguments:
    df - dataframe object containing the nz formatted datetime data.
    
    Returns:
    df - dataframe object with reformatted datetime information."""
    logging.info('Parsing datetime data...')
    
    #rename the year field:
    df = df.rename(columns = {'crashYear': 'year'})
    
    
    #assign approximate false because information provided is sufficient.
    df = df.assign(approximate = True)
    
    #generate id using string operations. - truly this is pitiful
    df['date_time_id'] = (df['year'].apply(str) + '-' + '-' + '-' + '-' )
    
    return df
    
def nz_location_handler(df):
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
    df['lat_long'] = df[['X', 'Y']].apply(lambda x: (x[0], x[1]), axis=1)
    
    #assign country and state
    df = df.assign(country = 'NZ')
    df = df.assign(state = 'NZ')
        
    #rename fields
    df = df.rename(columns = {'X': 'latitude', 'Y':'longitude', 'tlaName': 'local_government_area', 'areaUnitID': 'statistical_area'})
    
    return df
    
    
def nz_main():
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
    nz_df = pd.read_csv(s3_path+'/crash_nz/crash_nz.csv')
    
    #field harmonisation
    logging.info('Harmonising and renaming fields...')
    nz_df = nz_vehicle_summary_harmoniser(nz_df)
    nz_df = nz_casualties_summary_harmoniser(nz_df)
    nz_df = nz_description_harmoniser(nz_df)

    #field name changing
    nz_df = nz_datetime_handler(nz_df)
    nz_df = nz_location_handler(nz_df)
    
    #generate the primary key field.
    logging.info('Generating IDs...')
    nz_df['crash_id'] = nz_df['OBJECTID'].astype(str).apply(lambda x: 'NZ'+x)
    logging.info('Coercing to final staging structure...')
    nz_df = structure_checker(nz_df, expected_fields, coerce = True)
    
    return nz_df


if __name__ is '__main__':
    nz_main()