import pyproj
import pandas as pd
import numpy as np
import logging

#declare some proj strings - these are for coordinate transformation for SA and VIC data
sa_proj_string = "+proj=lcc +lon_0=135 +lat_0=-32 +lat_1=-28 +lat_2=-36 +x_0=1000000 +y_0=2000000"
vic_proj_string = "+proj=tmerc +lat_0=0 +lon_0=145 +k=1 +x_0=2500000 +y_0=6596534.558457338 +f=0.003352891869237217 +a=6378160 +b=6356774.719 +no_defs"

def map_coord_transformer(df, proj_string, lat_column_name, long_column_name):
    """
    Function which transforms individual jurisdictions' preferred coordinate system to a standard latitude and longitude format.
    ---
    
    Keyword Arguments:
    df - dataframe object containing the coordinates to be transformed.
    proj_string - projection string from the proj library. These were developed in the notebook file and are hardcoded as variables to allow users to simply select 
                  rather than be forced to manually supply.
    lat_column_name - a string which is the name of the x coordinate column - The coordinate which will be latitude.
    long_column_name - a string which is the name of the y coordinate column - The coordinate which will be longitude.
    
    Returns:
    df - dataframe with two additional columns calc_lat and calc_long for later manipulation. 
    """
    logging.info('Generating coordinate reference systems... ')
    #generate coordinate reference system objects for details of how this works 
    from_crs = pyproj.CRS.from_string(proj_string)
    from_proj = pyproj.Proj(from_crs)
    gps_proj = pyproj.Proj('epsg:4326')
    original_coordinates_to_latlong_obj = pyproj.Transformer.from_proj(from_proj, gps_proj)
    logging.info('Defining transformation functions...')
    def original_coordinates_to_latlong(adf):
        (lat,long) = original_coordinates_to_latlong_obj.transform(adf[lat_column_name], adf[long_column_name])
        return lat, long
    
    #apply converter to generate series
    logging.info('Converting coordinates...')
    latlong_series = df.apply(original_coordinates_to_latlong, axis=1)
    
    #get calculated values and put back into df.
    logging.info('Splitting series...')
    lat_series = latlong_series.copy().apply(lambda x: x[0])
    long_series = latlong_series.copy().apply(lambda x: x[1])
    
    #return the values as 
    logging.info('Preparing to return calc_lat and calc_long...')
    df.loc[:,'calc_lat'] = lat_series.copy()
    df.loc[:,'calc_long'] = long_series.copy()
    
    return df

def vehicles_id_generator(df):
    """
    Function takes the harmonised field names and produces the structured id in the form discussed in the documentation.
    ---
    
    Keyword Arguments:
    df -- pandas dataframe object which contains the summary unit information for the south australian dataset. 
    
    Returns:
    unit_summary_df -- Pandas dataframe object which contains the information harmonised to the preferred type list. Original fields will have been removed.
    """

    #find out which fields exist - use as integer later to remove fields which aren't valuable.
    animals_exist = (df['animals'] > 0)
    car_sedan_exist = (df['car_sedan'] > 0)
    car_utility_exist = (df['car_utility'] > 0)
    car_van_exist = (df['car_van'] > 0)
    car_4x4_exist = (df['car_4x4'] > 0)
    car_station_wagon_exist = (df['car_station_wagon'] > 0)
    motor_cycle_exist = (df['motor_cycle'] > 0)
    truck_small_exist = (df['truck_small'] > 0)
    truck_large_exist = (df['truck_large'] > 0)
    bus_exist = (df['bus'] > 0)
    taxi_exist = (df['taxi'] > 0)
    bicycle_exist = (df['bicycle'] > 0)
    scooter_exist = (df['scooter'] > 0)
    pedestrian_exist = (df['pedestrian'] > 0)
    inanimate_exist = (df['inanimate'] > 0)
    train_exist = (df['train'] > 0)
    tram_exist = (df['tram'] > 0)
    vehicle_other_exist = (df['vehicle_other'] > 0)
    
    #set the string variables to be appended
    animals_string = 'a'
    car_sedan_string = 'c'
    car_utility_string = 'u'
    car_van_string = 'v'
    car_4x4_string = 'f'
    car_station_wagon_string = 'w'
    motor_cycle_string = 'mc'
    truck_small_string = 't'
    truck_large_string = 'T'
    bus_string = 'B'
    taxi_string = 'x'
    bicycle_string = 'b'
    scooter_string = 's'
    pedestrian_string = 'p'
    inanimate_string = 'i'
    train_string = 'n'
    tram_string = 'm'
    vehicle_other_string = 'o'
    
    #create the vehicle id string after the following fashion - sum(Exists*(number_type+string_type))
    vehicles_id = (animals_exist*(f"{df['animals']}"+animals_string)+
                   car_sedan_exist*(f"{df['car_sedan']}"+car_sedan_string)+
                   car_utility_exist*(f"{df['car_utility']}"+car_utility_string)+
                   car_van_exist*(f"{df['car_van']}"+car_van_string)+
                   car_4x4_exist*(f"{df['car_4x4']}"+car_4x4_string)+
                   car_station_wagon_exist*(f"{df['car_station_wagon']}"+car_station_wagon_string)+
                   motor_cycle_exist*(f"{df['motor_cycle']}"+motor_cycle_string)+
                   truck_small_exist*(f"{df['truck_small']}"+truck_small_string)+
                   truck_large_exist*(f"{df['truck_large']}"+truck_large_string)+
                   bus_exist*(f"{df['bus']}"+bus_string)+
                   taxi_exist*(f"{df['taxi']}"+taxi_string)+
                   bicycle_exist*(f"{df['bicycle']}"+bicycle_string)+
                   scooter_exist*(f"{df['scooter']}"+scooter_string)+
                   pedestrian_exist*(f"{df['pedestrian']}"+pedestrian_string)+
                   inanimate_exist*(f"{df['inanimate']}"+inanimate_string)+
                   train_exist*(f"{df['train']}"+train_string)+
                   tram_exist*(f"{df['tram']}"+tram_string)+
                   vehicle_other_exist*(f"{df['vehicle_other']}"+vehicle_other_string))
                   
    return vehicles_id

def casualties_id_generator(df):
    """
    Function takes the harmonised field names and produces the structured id in the form discussed in the documentation.
    ---
    
    Keyword Arguments:
    df -- pandas dataframe object which contains the casualty information. 
    
    Returns:
    unit_summary_df -- Pandas dataframe object which contains casualties_id_field.
    """
    #declare strings
    casualties_string = 'c'
    fatalities_string = 'f'
    serious_injuries_string = 's'
    minor_injuries_string = 'm'
    
    #determine existence of three optional parameters
    fatalities_exist = (df['fatalities']>0)
    serious_injuries_exist = (df['serious_injuries']>0)
    minor_injuries_exist = (df['minor_injuries']>0)
    
    #calculate the string according to casualties+casualties_string+sum(exists*(count_type+type_string))
    casualties_id = (f"{df['casualties']}"+casualties_string+
                     fatalities_exist*(f"{df['fatalities']}"+fatalities_string)+
                     serious_injuries_exist*(f"{df['serious_injuries']}"+serious_injuries_string)+
                     minor_injuries_exist*(f"{df['minor_injuries']}"+minor_injuries_string))
    
    return casualties_id


def structure_checker(df, expected_fields, coerce = True, add_only = False):
    """
    Function which takes a dataframe as an argument and either advises gaps in it compared with the 'ideal' staging table or coerces the structure to the correct format.
    ---
    
    Keyword Arguments:
    df -- pandas dataframe objected to be compared
    expected_fields -- numpy array object containing the expected field names
    coerce -- boolean. If this is set to true, the structure will be coerced to have the correct field names. Logging warnings will be output.
    add_only -- boolean. If true, fields will only be added to ensure that the resulting dataframe contains what is in expected_fields (but may contain more). If false
                it will remove extraneous fields when returning. 
    
    Returns:
    df -- dataframe object which has been appropriately manipulated.
    
    """
    logging.info('Comparing expected and actual structure...')
    #what's absent?
    missing_fields = np.setdiff1d(expected_fields, df.columns)
    
    logging.warning('The following fields are missing: ' + str(missing_fields))
    
    if coerce:
        logging.info('Coercing structure to ideal...')
        #force the structure to be ideal.
        for field in missing_fields:
            df[field] = pd.np.nan
        if not add_only:
            logging.info('Removing additional fields...')
            df = df[expected_fields]
    
    return df
    
#create some dictionaries which manage correspondences
severity_dict = {#property_damage
                 '1: PDO': 'property_damage', '4': 'property_damage', 'PDO Major': 'property_damage', 'PDO Minor': 'property_damage', 
                 'Property Damage Only': 'property_damage', 'N': 'property_damage', 'Property damage only': 'property_damage',
                 #minor_injury
                 '2: MI': 'minor_injury', '3':'minor_injury', 'Medical': 'minor_injury', 'M':'minor_injury', 'Medical treatment': 'minor_injury', 
                 'Minor injury': 'minor_injury',
                 #serious_injury
                 '3: SI': 'serious_injury', '2': 'serious_injury', 'Hospital': 'serious_injury', 'Injury':'serious_injury', 'S': 'serious_injury', 
                 'Hospitalisation': 'serious_injury',
                 #fatality
                 '4: Fatal': 'fatality', '1': 'fatality', 'Fatal':'fatality', 'F': 'fatality'}


midblock_dict = {#This dictionary implicitly assumes that something is *either* a midblock *or* an intersection. This dictionary defines midblock and can be inverted to 
                #find intersections. 
                #midblocks
                'Not Divided': True, 'One Way': True, 'Divided Road': True, 'Freeway': True, 'Other': True, 'Not at intersection': True, 'Unknown': True, 
                'Dead end': True, 'Private property':True, 'Mid Block': True, 'YES':True, 'Midblock':True, 'No Roadway Feature': True, 'Bridge/Causeway': True, 
                'Forestry/National Park Road': True, 'Other': True, 
                #intersections
                'T-Junction': False, 'Cross Road': False, 'Pedestrian Crossing': False, 'Multiple': False, 'Y-Junction': False, 'Rail Xing': False, 'Interchange': False,
                'Rail Crossing': False, 'Crossover': False, 'Ramp On': False,  'Ramp Off': False,  'Multiple intersection': False, 'Y intersection': False, 
                'Road closure': False, 'Intersection': False, 'NO': False, 'Cross intersection': False, 'T intersection': False, 'Intersection - Cross': False,
                'Intersection - T-Junction': False, 'Intersection - Roundabout': False, 'Railway Crossing': False, 'Median Opening': False,
                'Intersection - Interchange': False, 'Intersection - Y-Junction': False, 'Intersection - Multiple Road': False, 'Merge Lane': False, 'Bikeway': False,
                'Intersection - 5+ way': False}

#taking a new tack now - this harmonisation step is a pain - let's try dict from keys

road_position_horizontal_dict = {
                                 **dict.fromkeys(['Straight', 'Straight Road', 'Straight road'], 'straight'), 
                                 **dict.fromkeys(['Curved - view open', 'Easy Curve', 'Moderate Curve', 'CURVED, VIEW OPEN'], 'curved_view_open'),
                                 **dict.fromkeys(['Curved - view obscured', 'Severe Curve', 'CURVED, VIEW OBSCURED'], 'curved_view_obscure'),
                                 **dict.fromkeys(['Unknown'], 'unknown')
                                }


road_position_vertical_dict = {
                                 **dict.fromkeys(['Level', 'Flat'], 'level'), 
                                 **dict.fromkeys(['Crest', 'Crest of Hill'], 'crest'),
                                 **dict.fromkeys(['Grade', 'Slope', 'Hill'], 'slope'),
                                 **dict.fromkeys(['Dip', 'Bottom of Hill'], 'dip'),
                                 **dict.fromkeys(['Unknown'], 'unknown')
                              }

road_sealed_dict = {
                    **dict.fromkeys(['Sealed - Dry', 'Sealed - Wet', 'Sealed', 'Good dry surface', 'Wet surface', 'Snow or ice', 'Muddy or oily surface'], True), 
                    **dict.fromkeys(['Unsealed - Dry', 'Unsealed - Wet', 'Unsealed', 'Loose surface'], False),
                    **dict.fromkeys(['Unknown'], None)
                    }


road_wet_dict = {
                    **dict.fromkeys(['Sealed - Wet', 'Unsealed - Wet', 'Ice/ Snow', 'Wet', 'Wet surface', 'Snow or ice', 'Muddy or oily surface'], True), 
                    **dict.fromkeys(['Sealed - Dry', 'Unsealed - Dry', 'Dry', 'Good dry surface', 'Loose surface'], False),
                    **dict.fromkeys(['Unknown'], None)
                    }


weather_dict = {
                **dict.fromkeys(['Clear', 'Fine', 'Not Raining'], 'fine'), 
                **dict.fromkeys(['Smoke/Dust', 'Smoke', 'Dust', 'Smoke or dust'], 'smoke_dust'),
                **dict.fromkeys(['Fog'], 'fog'),
                **dict.fromkeys(['Raining', 'Heavy Rain', 'Light Rain', 'Heavy rain', 'Light rain'], 'rain'),
                **dict.fromkeys(['Snowing', 'Snow', 'Snow or sleet'], 'snow'),
                **dict.fromkeys(['Mist'], 'mist'),
                **dict.fromkeys(['Strong winds'], 'high_wind'),
                **dict.fromkeys(['Cloudy or Overcast'], 'overcast'),
                **dict.fromkeys(['Other'], 'other'),
                **dict.fromkeys(['Unknown', 'Not known'], 'unknown')
                }

lighting_dict = {
                **dict.fromkeys(['Daylight', 'Bright Sun', 'Overcast', 'Day'], 'daylight'),
                **dict.fromkeys(['Dark - good street lighting', 'Dark Street lights on', 'Darkness - Lighted'], 'darkness_lit'),
                **dict.fromkeys(['Darkness - Not lighted', 'Dark No street lights', 'Dark',  'Night', 'Dark Street lights off', 'Dark - poor street lighting',
                                 'Dark - no street lights'], 'darkness_not_lit'),
                **dict.fromkeys(['Dawn/Dusk', 'Semi-darkness', 'Twilight', 'Dusk/Dawn'], 'dawn_dusk'),
                **dict.fromkeys(['Dark Street lights unknown'], 'other'),
                **dict.fromkeys(['Unknown'], 'unknown')
                }

traffic_controls_dict = {
                         **dict.fromkeys(['No traffic control', 'Nil', 'No Control'], 'none'),
                         **dict.fromkeys(['Stop sign', 'Stop Sign'], 'stop_sign'),
                         **dict.fromkeys(['Operating traffic lights', 'Traffic Signal', 'Traffic Signals'], 'traffic_lights'),
                         **dict.fromkeys(['Railway - lights only', 'Railway - lights and boom gate', 'Railway crossing sign', 'Rail Xing-Traffic Signals',
                                          'Rail Xing - Boom', 'Rail Xing - Flashing', 'Rail Xing - No Control'], 'railway_crossing'),
                         **dict.fromkeys(['Give way sign', 'Give Way Sign', 'Roundabout'], 'giveway_sign'),
                         **dict.fromkeys(['Pedestrian operated lights', 'Pedestrian crossing sign'], 'pedestrian_crossing'),
                         **dict.fromkeys(['Supervised school crossing', 'School crossing - flags', 'School Patrol'], 'school_crossing'),
                         **dict.fromkeys(['Road/Rail worker', 'Police', 'Points Man' ], 'manual_control'),
                         **dict.fromkeys(['Flashing amber lights', 'Miscellaneous', 'LATM device', 'Other'], 'other')
                        }

#some date dictionaries to make the date manipulations easier.

month_dict = {'January': 1, 
              'February': 2,
              'March': 3,
              'April': 4, 
              'May': 5,
              'June': 6,
              'July': 7,
              'August': 8,
              'September': 9,
              'October': 10,
              'November': 11,
              'December': 12}
weekday_dict = {'Sunday': 7, 
              'Monday': 1,
              'Tuesday': 2,
              'Wednesday': 3, 
              'Thursday': 4,
              'Friday': 5,
              'Saturday': 6}

#expected fields for some tables at some points. 
expected_fields = np.array(['crash_id', 'lat_long', 'date_time_id', 'description_id', 'vehicles_id', 'casualties_id', 'latitude', 'longitude', 'country', 'state',
                            'local_government_area', 'statistical_area', 'suburb', 'year', 'month', 'day_of_week', 'day_of_month', 'hour', 'approximate',
                            'animals', 'car_sedan', 'car_utility', 'car_van', 'car_4x4', 'car_station_wagon', 'motor_cycle', 'truck_small', 
                            'truck_large', 'bus', 'taxi', 'bicycle', 'scooter', 'pedestrian', 'inanimate', 'train', 'tram', 'vehicle_other', 'casualties',
                            'fatalities', 'serious_injuries', 'minor_injuries', 'severity', 'speed_limit', 'midblock', 'intersection', 'road_position_horizontal',
                            'road_position_vertical', 'road_sealed', 'road_wet', 'weather', 'crash_type', 'lighting', 'traffic_controls', 'drugs_alcohol', 
                            'DCA_code', 'comment'])

expected_vehicle_fields = np.array(['animals', 'car_sedan', 'car_utility', 'car_van', 'car_4x4', 'car_station_wagon', 'motor_cycle', 'truck_small', 
                                    'truck_large', 'bus', 'taxi', 'bicycle', 'scooter', 'pedestrian', 'inanimate', 'train', 'tram', 'vehicle_other'])

#fields expected to be in each of the final tables.
expected_crash_fields = np.array(['crash_id', 'lat_long', 'date_time_id', 'description_id', 'vehicles_id', 'casualties_id'])
                            
expected_location_fields = np.array(['lat_long', 'latitude', 'longitude', 'country', 'state', 'local_government_area', 'statistical_area', 'suburb'])
                                     
expected_datetime_fields = np.array(['date_time_id', 'year', 'month', 'day_of_week', 'day_of_month', 'hour', 'approximate'])
                                     
expected_vehicle_tab_fields = np.array(['vehicles_id', 'animals', 'car_sedan', 'car_utility', 'car_van', 'car_4x4', 'car_station_wagon', 'motor_cycle', 'truck_small', 
                                        'truck_large', 'bus', 'taxi', 'bicycle', 'scooter', 'pedestrian', 'inanimate', 'train', 'tram', 'vehicle_other'])

expected_casualty_fields = np.array(['casualties_id', 'casualties', 'fatalities', 'serious_injuries', 'minor_injuries'])

expected_description_fields = np.array(['description_id', 'severity', 'speed_limit', 'midblock', 'intersection', 'road_position_horizontal',
                            'road_position_vertical', 'road_sealed', 'road_wet', 'weather', 'crash_type', 'lighting', 'traffic_controls', 'drugs_alcohol', 
                            'DCA_code', 'comment'])