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

def sa_data_loader(year_start = 2012, year_end = 2019):
    """
    Function which loads the two relevant table types into memory as pandas dataframes from the s3 storage location.
    ---
    
    Keyword Arguments:
    year_start (int) -- The first year to be read in from the s3 storage location.
    year_end (int) -- The final year to be read in from the s3 storage location.
    
    Returns:
    crash_df -- data stored in s3 buckets as {year}_DATA_SA_Crash.csv files. This contains information regarding most parts of the crash.
    unit_df -- data stored in s3 buckets as {year}_DATA_SA_Units.csv files. This contains information regarding vehicles involved in the crash.
    """
    #declare file strings for subsequent format manipulation.
    units_file_string = s3_path + "/crash_sa/road-crash-data-{}/{}_DATA_SA_Units.csv"
    crash_main_file_string = s3_path + "/crash_sa/road-crash-data-{}/{}_DATA_SA_Crash.csv"
    
    #initialise two dataframes for the first year and then loop the rest
    unit_df = pd.read_csv(units_file_string.format(year_start, year_start), low_memory=False)
    crash_df = pd.read_csv(crash_main_file_string.format(year_start, year_start), low_memory=False)
    
    #initialise year range for the remaining years sought and loop through
    year_range = range(year_start+1, year_end+1)
    for year in year_range:
        #read in the next year's data
        next_year_crash_df = pd.read_csv(crash_main_file_string.format(year, year), low_memory=False)
        next_year_unit_df = pd.read_csv(units_file_string.format(year, year), low_memory=False)
        
        #join to existing table
        unit_df = pd.concat([unit_df, next_year_unit_df])
        crash_df = pd.concat([crash_df, next_year_crash_df])
    return crash_df, unit_df

def sa_unit_summariser(df):
    """
    Function which takes the unit data frame once read in and summarises the information contained at the level of unit type.
    ---
    
    Keyword Arguments:
    df -- pandas dataframe object which contains the unit information for the south australian dataset. 
    
    Returns:
    unit_summary_df -- Pandas dataframe object which contains the information summarised to the unit type level.
    """
    unit_summary_df = (df
                         [['REPORT_ID','Unit Type', 'Unit No']]
                         .groupby(['REPORT_ID', 'Unit Type']).count()
                         .rename(columns = {'Unit No': 'Count'}).reset_index()
                         .pivot_table(index='REPORT_ID', columns='Unit Type', values='Count', aggfunc=sum, fill_value=0))
    return unit_summary_df

def sa_vehicle_summary_harmoniser(df):
    """
    Function which maps vehicle type fields to the vehicle types preferred in the dataset.
    ---
    
    Keyword Arguments:
    df -- pandas dataframe object which contains the summary unit information for the south australian dataset. 
    
    Returns:
    df -- Pandas dataframe object which contains the information harmonised to the preferred type list. Original fields will have been removed.
    """
    logging.info('Harmoninsing vehicles data...')
    #declare mappings
    logging.info('Declaring mappings...')
    animal_fields = ['Animal - Domestic - Not Ridden',
                     'Animal - Wild', 
                     'Animal Drawn Vehicle', 
                     'Ridden Animal']
    car_sedan_fields = ['Motor Cars - Sedan']
    car_utility_fields = ['Utility']
    car_van_fields = ['Forward Control Passenger Van', 
                      'Panel Van']
    car_4x4_fields = ['Motor Cars - Tourer']
    car_station_wagon_fields = ['Station Wagon']
    motor_cycle_fields = ['Motor Cycle']
    truck_small_fields = ['Light Truck LT 4.5T']
    truck_large_fields = ['BDOUBLE - ROAD TRAIN', 
                          'RIGID TRUCK LGE GE 4.5T', 
                          'SEMI TRAILER'] 
    bus_fields = ['OMNIBUS']
    taxi_fields = ['Taxi Cab']
    bicycle_fields = ['Pedal Cycle', 
                      'Power Asst. Bicycle']
    scooter_fields = ['Scooter']
    pedestrian_fields = ['Motorised Wheelchair/Gopher', 
                         'Pedestrian on Footpath/Carpark', 
                         'Pedestrian on Road', 
                         'Small Wheel Vehicle User', 
                         'Wheelchair / Elec. Wheelchair']
    inanimate_fields = ['Bridge', 
                        'Guard Rail', 
                        'Other Fixed Obstruction', 
                        'Other Inanimate Object', 
                        'Pole - not Stobie', 
                        'Tree', 'Sign Post', 
                        'Stobie Pole', 
                        'Traffic Signal Pole',
                        'Wire Rope Barrier']
    train_fields = ['Railway Vehicle']
    tram_fields = ['Tram']
    vehicle_other_fields = ['Motor Vehicle - Type Unknown', 
                            'Other Defined Special Vehicle']
    
    logging.info('Totaling values...')
    #apply the mapping by summing columns in each field name. 
    df['animals'] = df[animal_fields].apply(sum, axis=1).fillna(0)
    df['car_sedan'] = df[car_sedan_fields].apply(sum, axis=1).fillna(0)
    df['car_utility'] = df[car_utility_fields].apply(sum, axis=1).fillna(0)
    df['car_van'] = df[car_van_fields].apply(sum, axis=1).fillna(0)
    df['car_4x4'] = df[car_4x4_fields].apply(sum, axis=1).fillna(0)
    df['car_station_wagon'] = df[car_station_wagon_fields].apply(sum, axis=1).fillna(0)
    df['motor_cycle'] = df[motor_cycle_fields].apply(sum, axis=1).fillna(0)
    df['truck_small'] = df[truck_small_fields].apply(sum, axis=1).fillna(0)
    df['truck_large'] = df[truck_large_fields].apply(sum, axis=1).fillna(0)
    df['bus'] = df[bus_fields].apply(sum, axis=1).fillna(0)
    df['taxi'] = df[taxi_fields].apply(sum, axis=1).fillna(0)
    df['bicycle'] = df[bicycle_fields].apply(sum, axis=1).fillna(0)
    df['scooter'] = df[scooter_fields].apply(sum, axis=1).fillna(0)
    df['pedestrian'] = df[pedestrian_fields].apply(sum, axis=1).fillna(0)
    df['inanimate'] = df[inanimate_fields].apply(sum, axis=1).fillna(0)
    df['train'] = df[train_fields].apply(sum, axis=1).fillna(0)
    df['tram'] = df[tram_fields].apply(sum, axis=1).fillna(0)
    df['vehicle_other'] = df[vehicle_other_fields].apply(sum, axis=1).fillna(0)
    
    logging.info('Generating IDs...')
    #generate IDs 
    df['vehicles_id'] = df.apply(vehicles_id_generator, axis=1)
    
    #dump the originals
    dump_fields = [animal_fields, car_sedan_fields, car_utility_fields, car_van_fields, car_4x4_fields, car_station_wagon_fields, motor_cycle_fields, truck_small_fields,
                   truck_large_fields, bus_fields, taxi_fields, bicycle_fields, scooter_fields, pedestrian_fields, inanimate_fields, train_fields, tram_fields, 
                   vehicle_other_fields]
    for field_list in dump_fields:
        df = df.drop(columns = field_list)
        
    return df
    
def sa_casualties_summary_harmoniser(df):
    """
    Function which maps casualty type fields to the casualty types preferred in the dataset.
    ---
    
    Keyword Arguments:
    df -- pandas dataframe object which contains the casualty information for the south australian dataset. 
    
    Returns:
    df -- Pandas dataframe object which contains the information harmonised to the preferred type list. Original fields will have been removed.
    """
    
    logging.info('Harmoninsing casualties data...')
    #first rename some columns
    df = df.rename(columns={'Total Cas':'casualties', 'Total Fats':'fatalities', 'Total SI':'serious_injuries', 'Total MI':'minor_injuries'})
    
    #now generate the id field
    df['casualties_id'] = df.apply(casualties_id_generator, axis=1)
    
    return df
    
def sa_description_harmoniser(df):
    """
    Function which accepts a merged sa dataset pandas dataframe and returns it with field descriptions harmonised.
    ---
    
    Keyword Arguments:
    df - dataframe object containing the sa formatted description data.
    
    Returns:
    df - dataframe object with reformatted description information.
    """
    
    logging.info('Harmoninsing description data...')

    #go down the list of things we can get for sa data
    #severity
    df['severity'] = df['CSEF Severity'].apply(lambda x: severity_dict[x])
    
    #intersection and midblock are opposites of one another.
    df['midblock'] = df['Position Type'].apply(lambda x: midblock_dict[x])
    df['intersection'] = (df['midblock']==False)
    
    #road positions
    df['road_position_horizontal'] = df['Horizontal Align'].apply(lambda x: road_position_horizontal_dict[x])
    df['road_position_vertical'] = df['Vertical Align'].apply(lambda x: road_position_vertical_dict[x])
    
    #road condition
    df['road_sealed'] = df['Road Surface'].apply(lambda x: road_sealed_dict[x])
    df['road_wet'] = df['Moisture Cond'].apply(lambda x: road_wet_dict[x])
    
    #other condition
    df['weather'] = df['Weather Cond'].apply(lambda x: weather_dict[x])
    df['lighting'] = df['DayNight'].apply(lambda x: lighting_dict[x])
    df['traffic_controls'] = df['Traffic Ctrls'].apply(lambda x: traffic_controls_dict[x])
    
    #rename: Area Speed --> speed_limit,  Crash Type --> crash_type, DUI Involved --> drugs_alcohol
    df = df.rename(columns = {'Area Speed': 'speed_limit', 'Crash Type': 'crash_type', 'DUI Involved': 'drugs_alcohol'})
    
    return df

def sa_datetime_handler(df):
    """
    Function which transforms the datetime data for the sa dataset
    ---
    
    Keyword Arguments:
    df - dataframe object containing the sa formatted datetime data.
    
    Returns:
    df - dataframe object with reformatted datetime information.
    """
    
    logging.info('Parsing datetime data...')
    df['month'] = df['Month'].apply(lambda x: month_dict[x])
    df['day_of_week'] = df['Day'].apply(lambda x: weekday_dict[x])
    df['hour'] = pd.to_datetime(df['Time']).dt.hour
    
    df = df.assign(day_of_month = pd.np.nan)
    df = df.assign(approximate = True)
    
    #rename
    df = df.rename(columns = {'Year': 'year'})
    
    df['date_time_id'] = (df['year'].apply(str) + '-' + df['month'].apply(str) + '-' + df['day_of_month'].fillna('') + '-' + df['day_of_week'].apply(str) 
                         + '-' + df['hour'].apply(str))
    
    return df
    
def sa_location_handler(df):
    """
    Function which transforms the location data in the sa dataset.
    ---
    
    Keyword Arguments:
    df - dataframe object containing the sa formatted location data.
    
    Returns:
    df - dataframe object with reformatted description information.
    """
    logging.info('Parsing location information...')
    df['lat_long'] = df[['calc_lat', 'calc_long']].apply(lambda x: (x[0], x[1]), axis=1)
    df = df.assign(country = 'AU')
    df = df.assign(state = 'SA')
    
    # on closer inspection the statistical area field is worthless - assign nan.
    df = df.assign(statistical_area = pd.np.nan)
    
    #rename fields
    df = df.rename(columns = {'calc_lat': 'latitude', 'calc_long':'longitude', 'LGA Name': 'local_government_area', 'Suburb': 'suburb'})
    
    return df
    
    
def sa_main():
    """
    Function which conducts the etl to the final staging structure for south australian data
    ---
    
    Keyword Arguments:
    None.
    
    Returns:
    Data frame containing the structure coerced appropriately to the correct format.
    """
    #read the datasets
    logging.info('Read in datasets...')
    sa_crash_df, sa_unit_df = sa_data_loader(year_start = 2012, year_end = 2018)
    
    #summarise the unit data
    logging.info('Summarise vehicle data...')
    sa_unit_summary_df = sa_unit_summariser(sa_unit_df)
    
    #merge the dataset
    logging.info('Merging datasets...')
    sa_merge_df = sa_crash_df.merge(sa_unit_summary_df, on='REPORT_ID', how = 'left')
    
    #coordinate translation
    logging.info('Transforming SA map coordinates...')
    sa_merge_df = map_coord_transformer(sa_merge_df, sa_proj_string, 'ACCLOC_X', 'ACCLOC_Y')
    
    #field harmonisation
    logging.info('Conduct field harmonisation and renaming...')
    sa_merge_df = sa_casualties_summary_harmoniser(sa_merge_df)
    sa_merge_df = sa_vehicle_summary_harmoniser(sa_merge_df)
    sa_merge_df = sa_description_harmoniser(sa_merge_df)
    
    #field name changing
    sa_merge_df = sa_datetime_handler(sa_merge_df)
    sa_merge_df = sa_location_handler(sa_merge_df)
    
    #generate the primary key field.
    logging.info('Assigning keys...')
    sa_merge_df['crash_id'] = sa_merge_df['REPORT_ID'].apply(lambda x: 'SA'+x)
    
    logging.info('Coercing to final staging table structure...')
    sa_merge_df = structure_checker(sa_merge_df, expected_fields = expected_fields, coerce = True)
    
    return sa_merge_df


if __name__ is '__main__':
    sa_main()