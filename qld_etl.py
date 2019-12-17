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


def qld_vehicle_summary_harmoniser(df):
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
    car_sedan_fields = ['Count_Unit_Car']
    car_utility_fields = []
    car_van_fields = []
    car_4x4_fields = []
    car_station_wagon_fields = []
    motor_cycle_fields = ['Count_Unit_Motorcycle_Moped']
    truck_small_fields = []
    truck_large_fields = ['Count_Unit_Truck'] 
    bus_fields = ['Count_Unit_Bus']
    taxi_fields = []
    bicycle_fields = ['Count_Unit_Bicycle']
    scooter_fields = []
    pedestrian_fields = ['Count_Unit_Pedestrian']
    inanimate_fields = []
    train_fields = []
    tram_fields = []
    vehicle_other_fields = ['Count_Unit_Other']
    
    logging.info('Summing values...')
    #apply the mapping by summing columns in each field name. 
    #df['animals'] = df[animal_fields].astype(int).apply(sum, axis=1).fillna(0)
    df['car_sedan'] = df[car_sedan_fields].astype(int).apply(sum, axis=1).fillna(0)
    #df['car_utility'] = df[car_utility_fields].astype(int).apply(sum, axis=1).fillna(0)
    #df['car_van'] = df[car_van_fields].apply(sum, axis=1).fillna(0)
    #df['car_4x4'] = df[car_4x4_fields].astype(int).apply(sum, axis=1).fillna(0)
    #df['car_station_wagon'] = df[car_station_wagon_fields].apply(sum, axis=1).fillna(0)
    df['motor_cycle'] = df[motor_cycle_fields].astype(int).apply(sum, axis=1).fillna(0)
    #df['truck_small'] = df[truck_small_fields].apply(sum, axis=1).fillna(0)
    df['truck_large'] = df[truck_large_fields].astype(int).apply(sum, axis=1).fillna(0)
    df['bus'] = df[bus_fields].astype(int).apply(sum, axis=1).fillna(0)
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
    
def qld_casualties_summary_harmoniser(df):
    """
    Function which maps casualty type fields to the casualty types preferred in the dataset.
    ---
    
    Keyword Arguments:
    df -- pandas dataframe object which contains the casualty information for the nz dataset. 
    
    Returns:
    df -- Pandas dataframe object which contains the information harmonised to the preferred type list. 
    """
    logging.info('Harmonising casualty data...')
    
    #calculate minor injuries from two predecessor fields
    df['minor_injuries'] = df['Count_Casualty_MedicallyTreated'] + df['Count_Casualty_MinorInjury']
    
    #rename
    df = df.rename(columns={'Count_Casualty_Fatality':'fatalities', 'Count_Casualty_Hospitalised':'serious_injuries', 
                            'Count_Casualty_Total':'casualties'})
    
    #now generate the id field
    df['casualties_id'] = df.apply(casualties_id_generator, axis=1)
    
    return df
    
def qld_description_harmoniser(df):
    """
    Function which accepts an nz dataset pandas dataframe and returns it with field descriptions harmonised.
    
    Keyword Arguments:
    df - dataframe object containing the nz formatted description data.
    
    Returns:
    df - dataframe object with reformatted description information."""
    logging.info('Harmonising description data...')
    
    #go down the list of things we can get for vic
    #severity - coerce to int bcause victoria records as int and dictionary accepts string keys
    df['severity'] = df['Crash_Severity'].apply(lambda x: severity_dict[x])
        
    #intersection and midblock are opposites of one another.
    df['midblock'] = df['Crash_Roadway_Feature'].apply(lambda x: midblock_dict[x])
    df['intersection'] = (df['midblock']==False)
    
    #road positions
    df['road_position_horizontal'] = df['Crash_Road_Horiz_Align'].apply(lambda x: road_position_horizontal_dict[x])
    df['road_position_vertical'] = df['Crash_Road_Vert_Align'].apply(lambda x: road_position_vertical_dict[x])
    
    #road_surface details
    df['road_sealed'] = df['Crash_Road_Surface_Condition'].apply(lambda x: road_sealed_dict[x])
    df['road_wet'] = df['Crash_Road_Surface_Condition'].apply(lambda x: road_wet_dict[x])
    
    #other condition
    df['weather'] = df['Crash_Atmospheric_Condition'].apply(lambda x: weather_dict[x])
    df['lighting'] = df['Crash_Lighting_Condition'].apply(lambda x: lighting_dict[x])
    df['traffic_controls'] = df['Crash_Traffic_Control'].apply(lambda x: traffic_controls_dict[x])
    
    #rename: speedLimit --> speed_limit
    df = df.rename(columns = {'Crash_Speed_Limit': 'speed_limit'})
    
    return df

def qld_datetime_handler(df):
    """
    Function which transforms the datetime data in the nz dataset.
    ---
    
    Keyword Arguments:
    df - dataframe object containing the nz formatted datetime data.
    
    Returns:
    df - dataframe object with reformatted datetime information."""
    logging.info('Parsing datetime data...')
    
    #assign approximate false because information provided is sufficient.
    df['month'] = df['Crash_Month'].apply(lambda x: month_dict[x])
    df['day_of_week'] = df['Crash_Day_Of_Week'].apply(lambda x: weekday_dict[x])
    
    df = df.assign(day_of_month = pd.np.nan)
    df = df.assign(approximate = True)
    
    #rename
    df = df.rename(columns = {'Crash_Year': 'year', 'Crash_Hour': 'hour'})
    
    df['date_time_id'] = (df['year'].apply(str) + '-' + df['month'].apply(str) + '-' + df['day_of_month'].fillna('') + '-' + df['day_of_week'].apply(str) 
                         + '-' + df['hour'].apply(str))
    
    return df
    
def qld_location_handler(df):
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
    df['lat_long'] = df[['Crash_Latitude_GDA94', 'Crash_Longitude_GDA94']].apply(lambda x: (x[0], x[1]), axis=1)
    
    #assign country and state
    df = df.assign(country = 'AU')
    df = df.assign(state = 'QLD')
        
    #rename fields
    df = df.rename(columns = {'Crash_Latitude_GDA94': 'latitude', 'Crash_Longitude_GDA94': 'longitude', 'Loc_Local_Government_Area': 'local_government_area', 
                              'Loc_ABS_Statistical_Area_2': 'statistical_area', 'Loc_Suburb': 'suburb'})
    
    return df
    
    
def qld_main():
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
    qld_df = pd.read_parquet(s3_path+'/crash_qld/crash')
    
    #field harmonisation
    logging.info('Harmonising and renaming fields...')
    qld_df = qld_vehicle_summary_harmoniser(qld_df)
    qld_df = qld_casualties_summary_harmoniser(qld_df)
    qld_df = qld_description_harmoniser(qld_df)

    #field name changing
    qld_df = qld_datetime_handler(qld_df)
    qld_df = qld_location_handler(qld_df)
    
    #generate the primary key field.
    logging.info('Generating IDs...')
    qld_df['crash_id'] = qld_df['Crash_Ref_Number'].astype(str).apply(lambda x: 'QLD'+x)
    logging.info('Coercing to final staging structure...')
    qld_df = structure_checker(qld_df, expected_fields, coerce = True)
    
    return qld_df


if __name__ is '__main__':
    qld_main()