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

def vic_data_loader():
    """
    Function which loads the two relevant table types into memory as pandas dataframes from the s3 storage location.
    ---
    
    Keyword Arguments:
    None.
    
    Returns:
    vic_df -- pandas dataframe containing the victorian crash data
    vic_node_df -- pandas dataframe containing the victorian node (location) data
    vic_atmos_df -- pandas dataframe containing the victorian atmospheric condition (weather) data
    vic_vehic_df -- pandas dataframe containing the victorian vehicle data
    
    """
    
    #load specific Vic files into memory - ACCIDENT, NODE, ATMOSPHERIC_COND, VEHICLE
    vic_df = pd.read_csv(s3_path + '/crash_vic/ACCIDENT.csv', low_memory = False)
    vic_node_df = pd.read_csv(s3_path + '/crash_vic/NODE.csv', low_memory = False)
    vic_atmos_df = pd.read_csv(s3_path + '/crash_vic/ATMOSPHERIC_COND.csv', low_memory = False)
    vic_vehic_df = pd.read_csv(s3_path + '/crash_vic/VEHICLE.csv', low_memory = False)
    
    return vic_df, vic_node_df, vic_atmos_df, vic_vehic_df

def vic_unit_summariser(df):
    """
    Function which takes the unit data frame once read in and summarises the information contained at the level of unit type.
    ---
    
    Keyword Arguments:
    df -- pandas dataframe object which contains the unit information for the victorian dataset. 
    
    Returns:
    unit_summary_df -- Pandas dataframe object which contains the information summarised to the unit type level.
    """
    vic_vehic_summary_df = (df
                                [['ACCIDENT_NO', 'Vehicle Type Desc', 'VEHICLE_ID']]
                                .groupby(['ACCIDENT_NO','Vehicle Type Desc'])
                                .count()
                                .rename(columns = {'VEHICLE_ID':'count'})
                                .reset_index()
                                .pivot_table(index = 'ACCIDENT_NO', columns = 'Vehicle Type Desc', values='count', aggfunc = sum, fill_value = 0)
                           )
    return vic_vehic_summary_df

def vic_vehicle_summary_harmoniser(df):
    """
    Function which maps vehicle type fields to the vehicle types preferred in the dataset.
    ---
    
    Keyword Arguments:
    df -- pandas dataframe object which contains the summary unit information for the victorian dataset. 
    
    Returns:
    df -- Pandas dataframe object which contains the information harmonised to the preferred type list. 
    """
    logging.info('Parsing vehicle data...')
    logging.info('Declaring mappings...')
    #declare mappings
    animal_fields = ['Horse (ridden or drawn)']
    car_sedan_fields = ['Car']
    car_utility_fields = ['Utility']
    car_van_fields = ['Panel Van']
    car_4x4_fields = []
    car_station_wagon_fields = ['Station Wagon']
    motor_cycle_fields = ['Motor Cycle', 'Quad Bike']
    truck_small_fields = ['Light Commercial Vehicle (Rigid) <= 4.5 Tonnes GVM']
    truck_large_fields = ['Heavy Vehicle (Rigid) > 4.5 Tonnes', 'Prime Mover (No of Trailers Unknown)', 'Prime Mover - Single Trailer','Prime Mover B-Double', 
                          'Prime Mover B-Triple', 'Prime Mover Only', 'Rigid Truck(Weight Unknown)'] 
    bus_fields = ['Bus/Coach', 'Mini Bus(9-13 seats)']
    taxi_fields = ['Taxi']
    bicycle_fields = ['Bicycle']
    scooter_fields = ['Moped', 'Motor Scooter']
    pedestrian_fields = []
    inanimate_fields = ['Not Applicable', 'Parked trailers']
    train_fields = ['Train']
    tram_fields = ['Tram']
    vehicle_other_fields = ['Other Vehicle', 'Plant machinery and Agricultural equipment', 'Unknown']

    logging.info('Summing fields...')
    #apply the mapping by summing columns in each field name. 
    df['animals'] = df[animal_fields].apply(sum, axis=1).fillna(0)
    df['car_sedan'] = df[car_sedan_fields].apply(sum, axis=1).fillna(0)
    df['car_utility'] = df[car_utility_fields].apply(sum, axis=1).fillna(0)
    df['car_van'] = df[car_van_fields].apply(sum, axis=1).fillna(0)
    #df['car_4x4'] = df[car_4x4_fields].apply(sum, axis=1).fillna(0)
    df['car_station_wagon'] = df[car_station_wagon_fields].apply(sum, axis=1).fillna(0)
    df['motor_cycle'] = df[motor_cycle_fields].apply(sum, axis=1).fillna(0)
    df['truck_small'] = df[truck_small_fields].apply(sum, axis=1).fillna(0)
    df['truck_large'] = df[truck_large_fields].apply(sum, axis=1).fillna(0)
    df['bus'] = df[bus_fields].apply(sum, axis=1).fillna(0)
    df['taxi'] = df[taxi_fields].apply(sum, axis=1).fillna(0)
    df['bicycle'] = df[bicycle_fields].apply(sum, axis=1).fillna(0)
    df['scooter'] = df[scooter_fields].apply(sum, axis=1).fillna(0)
    #df['pedestrian'] = df[pedestrian_fields].apply(sum, axis=1).fillna(0)
    df['inanimate'] = df[inanimate_fields].apply(sum, axis=1).fillna(0)
    df['train'] = df[train_fields].apply(sum, axis=1).fillna(0)
    df['tram'] = df[tram_fields].apply(sum, axis=1).fillna(0)
    df['vehicle_other'] = df[vehicle_other_fields].apply(sum, axis=1).fillna(0)
    
    #add in two missing fields using structure checker
    df = structure_checker(df, expected_vehicle_fields, coerce=True, add_only = True)
    
    #add in 0s
    df[expected_vehicle_fields] = df[expected_vehicle_fields].fillna(0).astype(int)
    
    #generate IDs 
    df['vehicles_id'] = df.apply(vehicles_id_generator, axis=1)
            
    return df
    
def vic_casualties_summary_harmoniser(df):
    """
    Function which maps casualty type fields to the casualty types preferred in the dataset.
    ---
    
    Keyword Arguments:
    df -- pandas dataframe object which contains the casualty information for the victorian dataset. 
    
    Returns:
    df -- Pandas dataframe object which contains the information harmonised to the preferred type list. 
    """
    
    logging.info('Harmonising casualty data...')
    
    #first rename some columns
    df = df.rename(columns={'NO_PERSONS_KILLED':'fatalities', 'NO_PERSONS_INJ_2':'serious_injuries', 'NO_PERSONS_INJ_3':'minor_injuries'})
    
    #get the casualties field
    df['casualties'] = df['fatalities'] + df['serious_injuries'] + df['minor_injuries']
    
    #now generate the id field
    df['casualties_id'] = df.apply(casualties_id_generator, axis=1)
    
    return df
    
def vic_description_harmoniser(df):
    """
    Function which accepts a merged sa dataset pandas dataframe and returns it with field descriptions harmonised.
    
    Keyword Arguments:
    df - dataframe object containing the victorian formatted description data.
    
    Returns:
    df - dataframe object with reformatted description information."""
    
    logging.info('Harmonising description data...')

    #go down the list of things we can get for vic
    #severity - coerce to int bcause victoria records as int and dictionary accepts string keys
    df['severity'] = df['SEVERITY'].astype(str).apply(lambda x: severity_dict[x])
    
    #intersection and midblock are opposites of one another.
    df['midblock'] = df['Road Geometry Desc'].apply(lambda x: midblock_dict[x])
    df['intersection'] = (df['midblock']==False)
    
    #other condition
    df['weather'] = df['Atmosph Cond Desc'].apply(lambda x: weather_dict[x])
    df['lighting'] = df['Light Condition Desc'].apply(lambda x: lighting_dict[x])
    
    #rename: SPEED_ZONE --> speed_limit,  Accident Type Desc --> crash_type, DCA_CODE --> DCA_code, DCA Description
    df = df.rename(columns = {'SPEED_ZONE': 'speed_limit', 'Accident Type Desc': 'crash_type', 'DCA_CODE': 'DCA_code', 'DCA Description': 'comment'})
    
    return df

def vic_datetime_handler(df):
    """
    Function which transforms the datetime data in the sa dataset.
    ---
    
    Keyword Arguments:
    df - dataframe object containing the victorian formatted datetime data.
    
    Returns:
    df - dataframe object with reformatted datetime information."""
    
    logging.info('Parsing datetime data...')
    
    #extract a datetime instance from the fields 'ACCIDENTDATE', 'ACCIDENTTIME'
    
    df['ACCIDENTDATETIME'] = df['ACCIDENTDATE'] + " " + df['ACCIDENTTIME']
    
    #introduce datetime variable
    df['datetime'] = df['ACCIDENTDATETIME'].str.strip().apply(lambda x: datetime.strptime(x, '%d/%m/%Y %H:%M:%S'))
    
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
    
def vic_location_handler(df):
    """
    Function which transforms the location data in the sa dataset.
    ---
    
    Keyword Arguments:
    df - dataframe object containing the victorian formatted location data.
    
    Returns:
    df - dataframe object with reformatted location information. 
    """
    logging.info('Parsing location data...')
    
    df['lat_long'] = df[['calc_lat', 'calc_long']].apply(lambda x: (x[0], x[1]), axis=1)
    
    df = df.assign(country = 'AU')
    df = df.assign(state = 'VIC')
        
    #rename fields
    df = df.rename(columns = {'calc_lat': 'latitude', 'calc_long':'longitude', 'LGA_NAME': 'local_government_area'})
    
    return df
    
    
def vic_main():
    """
    Function which conducts the etl to the final staging structure for victorian data
    ---
    
    Keyword Arguments:
    None.
    
    Returns:
    Data frame containing the structure coerced appropriately to the correct format.
    """
    #read the datasets
    logging.info('Read in datasets...')
    vic_df, vic_node_df, vic_atmos_df, vic_vehic_df = vic_data_loader()
    
    #summarise the vehicle data
    logging.info('Summarising vehicle information')
    vic_vehic_summary_df = vic_unit_summariser(vic_vehic_df)
    
    #merge the dataset - main crash, then node, then atmospheric, finally vehicle summary
    logging.info('Merging datasets...')
    vic_merge_df = (vic_df
                    .merge(vic_node_df, how='left', on='ACCIDENT_NO')
                    .merge(vic_atmos_df, on = 'ACCIDENT_NO', how='left')
                    .merge(vic_vehic_summary_df, how = 'left', on = 'ACCIDENT_NO'))
    
    
    
    #coordinate translation
    logging.info('Conducting coordinate translation...')
    vic_merge_df = map_coord_transformer(vic_merge_df, vic_proj_string, 'AMG_X', 'AMG_X')
    
    
    #field harmonisation
    logging.info('Harmonising and renaming fields...')
    vic_merge_df = vic_vehicle_summary_harmoniser(vic_merge_df)
    vic_merge_df = vic_casualties_summary_harmoniser(vic_merge_df)
    vic_merge_df = vic_description_harmoniser(vic_merge_df)

    #field name changing
    vic_merge_df = vic_datetime_handler(vic_merge_df)
    vic_merge_df = vic_location_handler(vic_merge_df)
    
    #generate the primary key field.
    logging.info('Generating crash IDs...')
    vic_merge_df['crash_id'] = vic_merge_df['ACCIDENT_NO'].apply(lambda x: 'VIC'+x)
    
    #coerce to final staging structure.
    logging.info('Coercing to final staging structure...')
    vic_merge_df = structure_checker(vic_merge_df, expected_fields, coerce = True)
    
    return vic_merge_df


if __name__ is '__main__':
    vic_main()