import glob
import sqlite3

import pandas as pd
import json
import os
import db_ops
import datetime


def convert_json_to_dict():
    """Convert the weather_data from a json type to python dictionary type"""
    filepath = os.path.dirname(os.path.abspath('convert_to_df.py)'))
    fileList = glob.glob(filepath + '/weather_data*')
    Date_of_Extraction = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
    for fullFilePath in fileList:
        filename = str(fullFilePath).split("/")[-1].split("'")[0]
        with open(filename, 'r', encoding='utf-8') as json_:
            json_file = json.load(json_)
            json_file = json.dumps(json_file)
            json_file = json.loads(json_file)
            convert_dict_to_df(json_file)
            os.rename(filename, str('processed_') + Date_of_Extraction + str('_') + filename)
    return json_file


def convert_dict_to_df(file):
    # json_ = convert_json_to_dict()
    """Convert the dict file type to a pandas dataframe"""
    df = pd.json_normalize(file)
    df = df.rename({'clouds.all': 'all_clouds', 'coord.lat': 'coord_lattitude', 'coord.lon': 'coord_longitude',
                    'main.feels_like': 'main_feels_like', 'main.humidity': 'main_humidity',
                    'main.sea_level': 'main_sea_level', 'main.grnd_level': 'main_ground_level',
                    'main.pressure': 'main_pressure', 'main.temp': 'main_temp', 'main.temp_max': 'main_temp_max',
                    'main.temp_min': 'main_temp_min', 'sys.id': 'sys_id', 'wind.deg': 'wind_degrees',
                    'rain.1h': 'rain', 'wind.speed': 'wind_speed', 'wind.gust': 'wind_gust', 'sys.type': 'type',
                    'sys.country': 'country', 'sys.sunrise': 'sunrise', 'sys.sunset': 'sunset'}, axis=1)
    df = df.astype({'weather': str})
    upload_to_db(df)
    return df


def upload_to_db(data):
    """Upload the dataframe to mysqlDB"""
    print('Creating db connection...')
    try:
        db_conn = db_ops.createConnection()
        data.to_sql('test_data_weather', db_conn, if_exists='append', index=False)
    except sqlite3.Error as err:
        print(err)
    else:
        print('Database updated successfully...')
        print('Connection closed...')


# # upload_to_db()
convert_json_to_dict()
