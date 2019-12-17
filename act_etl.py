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


def act_description_harmoniser(df):
    """
    Function which accepts an nz dataset pandas dataframe and returns it with field descriptions harmonised.
    
    Keyword Arguments:
    df - dataframe object containing the nz formatted description data.
    
    Returns:
    df - dataframe object with reformatted description information.
    """
    
    logging.info('Harmonising description data...')
    
    #go down the list of things we can get for vic
    #severity - coerce to int bcause victoria records as int and dictionary accepts string keys
    df['severity'] = df['crash_severity'].apply(lambda x: severity_dict[x])
    
    #build the crash type field out of EVENT_NATURE and EVENT_TYPE

    #intersection and midblock are opposites of one another.
    df['midblock'] = (df['midblock'].apply(lambda x: x=='YES'))
    df['intersection'] = (df['midblock']==False)

    
    #road condition
    df['road_sealed'] = df['road_condition'].apply(lambda x: road_sealed_dict[x])
    df['road_wet'] = df['road_condition'].apply(lambda x: road_wet_dict[x])
    
    #other condition
    df['weather'] = df['weather_condition'].apply(lambda x: weather_dict[x])
    df['lighting'] = df['lighting_condition'].apply(lambda x: lighting_dict[x])

    return df

def act_datetime_handler(df):
    """
    Function which transforms the datetime data in the nz dataset.
    ---
    
    Keyword Arguments:
    df - dataframe object containing the nz formatted datetime data.
    
    Returns:
    df - dataframe object with reformatted datetime information."""
    logging.info('Parsing datetime data...')
    
    #get dates and times from the format in which they have appeared in the dataset.
    df['date'] = df['crash_date'].str[0:10]
    df['time'] = df['crash_time'].dt.time.astype(str)
    
    #concat and convert to datetime
    df['datetime'] = ((df['date'] + ' ' + df['time'])
                      .str.strip()
                      .apply(lambda x: datetime.strptime(x, '%Y-%m-%d %H:%M:%S')))
    
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
    
def act_location_handler(df):
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
    df['lat_long'] = df[['latitude', 'longitude']].apply(lambda x: (x[0], x[1]), axis=1)
    
    #assign country and state
    df = df.assign(country = 'AU')
    df = df.assign(state = 'ACT')
        
    #rename fields
    df = df.rename(columns = {'suburb_location': 'suburb'})
    
    return df
    
    
def act_main():
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
    act_df = pd.read_parquet(s3_path + '/crash_act/crash')
    
    #field harmonisation
    logging.info('Harmonising and renaming fields...')
    act_df = act_description_harmoniser(act_df)

    #field name changing
    act_df = act_datetime_handler(act_df)
    act_df = act_location_handler(act_df)
    
    #generate the primary key field.
    logging.info('Generating IDs...')
    act_df['crash_id'] = act_df['crash_id'].astype(str).apply(lambda x: 'ACT'+x)
    
    logging.info('Coercing to final staging structure...')
    act_df = structure_checker(act_df, expected_fields, coerce = True)
    
    return act_df


if __name__ is '__main__':
    act_main()