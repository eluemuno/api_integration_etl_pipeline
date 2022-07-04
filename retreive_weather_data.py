import json
import os
import sqlite3
import yaml
import datetime
import random
import db_ops
import pandas as pd

import requests
from dotenv import load_dotenv


def getrandomcity():
    """Get a random city name from a pre-populated yaml file (cityList.yaml)"""
    cityList = os.path.dirname(os.path.abspath('cityList.yaml') + '/cityList.yaml')
    with open(cityList) as file:
        cities = yaml.load(file, Loader=yaml.FullLoader)
    return cities


def fetch_city_iso_from_db():
    """Search the db for the corresponding iso3 code using the random city name"""
    city = random.choice(getrandomcity()['cities'])
    # print(city)
    try:
        conn = db_ops.createConnection()
        query = """select city, iso3 from worldcities where city = ? LIMIT 1"""
        df = pd.read_sql_query(query, conn, params=[city])
        # #----------
        while True:
            if not df.empty:
                return df
            break
        # #----------
    except sqlite3.Error as err:
        print(err)
    else:
        return df


def get_city_name_from_coordinate():
    """Pass the city name and iso3 code to the geocode endpoint to get the corresponding lat and lon values.
    These would be required for the weather api as parameters."""
    load_dotenv()
    # city_name = sys.argv[1]
    # iso_country_code = sys.argv[2]
    city_name = fetch_city_iso_from_db()['city'].to_string(index=False)
    iso_country_code = fetch_city_iso_from_db()['iso3'].to_string(index=False)
    print('City is', city_name)
    print('Country code is', iso_country_code)
    url = 'http://api.openweathermap.org/geo/1.0/direct?q=' + city_name + ',' + iso_country_code + \
          '&appid=' + os.environ.get('api_token')
    json_data = requests.get(url).json()
    print('Getting converted coordinates...')
    formattedJson = json.dumps(json_data, sort_keys=True, indent=4)
    return formattedJson


def extract_coordinates():
    """Extract only the lat and lon values from the geocode response. These will be passed to the weather api
    as parameters """
    print('Extracting lat and lon values from payload...')
    coordinates = {}
    payload = json.loads(get_city_name_from_coordinate())
    for i in range(len(payload)):
        lat = payload[i]["lat"]
        lon = payload[i]["lon"]
        coordinates['lat'] = lat
        coordinates['lon'] = lon
        print('Activity Completed...')
    return coordinates


def extract_weather_data():
    """Extract weather data using the coordinates obtained through geocode api. The response, formatted in json,
    will then be written the output to an external file with server timestamp and internal file timestamp attached to
    filename for identification """
    print('Extracting weather data...')
    load_dotenv()
    coord = json.dumps(extract_coordinates())
    coord = json.loads(coord)
    lat = coord['lat']
    lon = coord['lon']
    url = 'https://api.openweathermap.org/data/2.5/weather?lat=' + str(lat) + '&lon=' + str(lon) + '&appid=' \
          + os.environ.get('api_token')
    response = requests.get(url).json()
    print('Done!')
    weather_data_json = json.dumps(response, indent=4)
    print('Activity Completed...')
    print('Write to file...')
    Date_of_Extraction = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
    filename = 'weather_data_' + Date_of_Extraction + '_' + str(json.loads(weather_data_json)['dt']) + str('.json')
    with open(filename, "w") as outfile:
        outfile.write(weather_data_json)
    return filename


extract_weather_data()