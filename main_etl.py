import logging
import pandas as pd
from datetime import datetime
from sa_etl import sa_main
from vic_etl import vic_main
from nz_etl import nz_main
from qld_etl import qld_main
from wa_etl import wa_main
from act_etl import act_main

from crash_utilities import expected_crash_fields, expected_location_fields, expected_datetime_fields, expected_vehicle_tab_fields, expected_casualty_fields
from crash_utilities import expected_description_fields 

import os
import configparser
import pyarrow

#read the config file to extract the aws credentials
config = configparser.ConfigParser()
config.read('config_file.cfg')
os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']

#set the base s3_path for subsequent manipulation 
s3_path = config['S3']['S3_BUCKET_PATH']

#initialise logging
logging.basicConfig(filename=f'file{datetime.now().strftime("%Y-%m-%d")}.log', filemode='w', format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', 
                    level=logging.DEBUG)

def etl_main():
    logging.info('Commencing ETL runs.')
    
    #south australia
    logging.info('Commencing SA data run')
    sa_merge_df = sa_main()
    
    #Victoria
    logging.info('Commencing Vic data run')
    vic_merge_df = vic_main()
    
    #New Zealand
    logging.info('Commencing NZ data run')
    nz_merge_df = nz_main()
    
    #Queensland
    logging.info('Commencing QLD data run')
    qld_merge_df = qld_main()
    
    #Western australia
    logging.info('Commencing WA data run')
    wa_merge_df = wa_main()
    
    #Australian Capital Territory data run
    logging.info('Commencing ACT data run')
    act_merge_df = act_main()
    
    logging.info('All state level ETL runs completed successfully.')
    
    #join all staging tables together
    logging.info('Merge state level datasets...')
    staging_df = pd.concat([sa_merge_df, vic_merge_df, nz_merge_df, qld_merge_df, wa_merge_df, act_merge_df], axis=0)
    
    logging.info('Generate serial numbe for description field...')
    #generate serial number for rows for description id - could use crash id as primary key for description table but this is somewhat nicer - prevents silly joins.
    #double reset index - and drop the original description id.
    staging_df = staging_df.reset_index(drop=True).reset_index().drop(columns=['description_id']).rename(columns = {'index':'description_id'})
    
    logging.info('Splitting table structure...')
    #split into tables and drop duplicates
    crash_df = staging_df[expected_crash_fields].drop_duplicates()
    description_df = staging_df[expected_description_fields].drop_duplicates()
    datetime_df = staging_df[expected_datetime_fields].drop_duplicates()
    casualties_df = staging_df[expected_casualty_fields].drop_duplicates()
    location_df = staging_df[expected_location_fields].drop_duplicates()
    vehicles_df = staging_df[expected_vehicle_tab_fields].drop_duplicates()
    
    logging.info('Success. Running count checks.')
    
    #one more dict for the road - get dict of name of table vs table object
    name_dict = {'Crash': crash_df, 'Description': description_df, 'DateTime': datetime_df, 'Casualties': casualties_df, 'Location': location_df, 
                 'Vehicles': vehicles_df}
    
    #run count check on each table. 
    for key in name_dict.keys():
        row_count = len(name_dict[key])
        logging.info(f'{key} table contains {row_count} rows.')
    
    #paths for writing - using both csv and parquet
    s3_csv_path = s3_path + '/Final/CSV/'
    s3_parquet_path = s3_path + '/Final/parquet/'
    
    #write to final bucket - csv first
    crash_df.to_csv(s3_csv_path + 'Crash.csv')
    description_df.to_csv(s3_csv_path + 'Description.csv')
    datetime_df.to_csv(s3_csv_path + 'DateTime.csv')
    casualties_df.to_csv(s3_csv_path + 'Casualties.csv')
    location_df.to_csv(s3_csv_path + 'Location.csv')
    vehicles_df.to_csv(s3_csv_path + 'Vehicles.csv')
    
    #now parquet
    crash_df.to_parquet(s3_parquet_path + 'Crash.csv')
    description_df.to_parquet(s3_parquet_path + 'Description.csv')
    datetime_df.to_parquet(s3_parquet_path + 'DateTime.csv')
    casualties_df.to_parquet(s3_parquet_path + 'Casualties.csv')
    location_df.to_parquet(s3_parquet_path + 'Location.csv')
    vehicles_df.to_parquet(s3_parquet_path + 'Vehicles.csv')
    
if __name__ is '__main__':
    etl_main()