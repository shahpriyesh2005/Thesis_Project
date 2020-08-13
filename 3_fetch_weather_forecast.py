#!/usr/bin/env python
import requests, json
from datetime import datetime
from pymongo import MongoClient

# API Key & URL
api_key = "bbee21b0489d90f69bf6d9b4235036a5"
base_url = "https://api.openweathermap.org/data/2.5/onecall?"

# Latitude & Longitude of New York 
latitude = "40.7143"
longitude = "-74.006"

# Database details
db_url = "localhost"
db_port = 27017
db_name = "thesisproject"
coll_name = "weather_forecast_data"

# Function to call the OpenWeatherMap API and fetch weather forecast
def get_weather_forecast():
    # Complete URL for API Call
    complete_url = base_url + "lat=" + latitude + "&lon=" + longitude + "&units=metric&exclude=current,minutely,hourly" + "&appid=" + api_key

    # Get response from the API
    weather_forecast_response = requests.get(complete_url)

    # Convert response to JSON 
    weather_forecast_json = weather_forecast_response.json()

    # Extract the daily weather data object from the JSON object
    dailyList = weather_forecast_json['daily']

    # Change the dates from Unix timestamp to readable timestamp
    for i in dailyList: 
        dt_unix_ts = int(i.get('dt'))
        dt_readable_ts = datetime.utcfromtimestamp(dt_unix_ts).strftime('%Y-%m-%d %H:%M:%S')
        i['dt'] = dt_readable_ts
        
        sunrise_unix_ts = int(i.get('sunrise'))
        sunrise_readable_ts = datetime.utcfromtimestamp(sunrise_unix_ts).strftime('%Y-%m-%d %H:%M:%S')
        i['sunrise'] = sunrise_readable_ts
        
        sunset_unix_ts = int(i.get('sunset'))
        sunset_readable_ts = datetime.utcfromtimestamp(sunset_unix_ts).strftime('%Y-%m-%d %H:%M:%S')
        i['sunset'] = sunset_readable_ts
        
        # Call the save_weather_forecast function to save the data in database
        save_weather_forecast(i)

# Function to save the data in the database
def save_weather_forecast(dailyForecastData):
    mongoClient = MongoClient(db_url, db_port)
    dbName = mongoClient[db_name]
    
    collName = dbName[coll_name]
    collName.insert(dailyForecastData)
    
# Call the get_weather_forecast function
get_weather_forecast()
print ("Weather forecast fetched successfully")
