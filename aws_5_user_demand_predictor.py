#!/usr/bin/env python
from pymongo import MongoClient
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, countDistinct
from pyspark.sql.types import IntegerType
from datetime import datetime, date
import pyspark.sql.functions as func
import csv
import math

# Database details
db_url = "mongodb://admin:admin@ec2-54-81-206-27.compute-1.amazonaws.com:27017/?authSource=admin&readPreference=primary&ssl=false"
db_port = 27017
db_name = "thesisproject"
coll_name = "weather_forecast_data"

mongoClient = MongoClient(db_url, db_port)
dbName = mongoClient[db_name]
collName = dbName[coll_name]

# Create a spark session to connect MongoDB using Spark-Mongo Connector
spark_session = SparkSession \
    .builder \
    .appName("myApp") \
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.11:2.4.1") \
    .config("spark.mongodb.input.uri", "mongodb://admin:admin@ec2-54-81-206-27.compute-1.amazonaws.com:27017/?authSource=admin&readPreference=primary&ssl=false") \
    .config("spark.mongodb.output.uri", "mongodb://admin:admin@ec2-54-81-206-27.compute-1.amazonaws.com:27017/?authSource=admin&readPreference=primary&ssl=false") \
    .getOrCreate()

# Load data from MongoDB collections to data frame
historical_weather_df = spark_session.read.format("com.mongodb.spark.sql.DefaultSource").option("uri", "mongodb://admin:admin@ec2-54-81-206-27.compute-1.amazonaws.com:27017/thesisproject.historical_weather_data?authSource=admin&readPreference=primary&ssl=false").load()
aggregated_historical_taxi_data_df = spark_session.read.format("com.mongodb.spark.sql.DefaultSource").option("uri", "mongodb://admin:admin@ec2-54-81-206-27.compute-1.amazonaws.com:27017/thesisproject.aggregated_historical_taxi_data?authSource=admin&readPreference=primary&ssl=false").load()

# Set alias of the data frame
hw = historical_weather_df.alias('hw')
aht_df = aggregated_historical_taxi_data_df.alias('aht_df')

# Fetch required columns from the weather forecast data
forecast_data = collName.find({ }, { "_id": 0, "dt": 1, "temp": 1, "weather": 1, "rain": 1 })

today = date.today()
result_list = []

for forecast_day in forecast_data:
    forecast_rain = 0
    forecast_rain_upper_limit = 0
    forecast_rain_lower_limit = 0
    aggregate_count = 0

    print("forecast_day: " + str(forecast_day))
    
    forecast_date = forecast_day.get('dt')
    forecast_date = datetime.strptime(forecast_date, '%Y-%m-%d %H:%M:%S').date()
    forecast_day_of_week = forecast_date.weekday()
    print("     forecast_day_of_week: " + str(forecast_day_of_week))
    
    if (forecast_date > today):
        if 'rain' in forecast_day:
            forecast_rain = forecast_day.get('rain')
            forecast_rain = forecast_rain / 25.4
            forecast_rain = round(forecast_rain, 2)
            forecast_rain_upper_limit = forecast_rain + (forecast_rain * 0.15)
            forecast_rain_lower_limit = forecast_rain - (forecast_rain * 0.15)
            forecast_rain_upper_limit = round(forecast_rain_upper_limit, 2)
            forecast_rain_lower_limit = round(forecast_rain_lower_limit, 2)
        else:
            forecast_rain = 0
            forecast_rain_upper_limit = 0
            forecast_rain_lower_limit = 0
        
        print("     forecast_rain: " + str(forecast_rain) + " forecast_rain_upper_limit: " + str(forecast_rain_upper_limit) + " forecast_rain_lower_limit: " + str(forecast_rain_lower_limit))
        
        # Select historical weather dates based on lower and upper limit of precipitation
        if forecast_rain == 0:
        	hw_df = hw.select(hw.DATE, hw.DailyPrecipitation).filter(func.col("DailyPrecipitation") == 0)
        else:
        	hw_df = hw.select(hw.DATE, hw.DailyPrecipitation).filter(func.col("DailyPrecipitation").between(forecast_rain_lower_limit, forecast_rain_upper_limit))
        
        if hw_df.count() > 0:
            hw_df = hw_df.withColumn('hw_date_formatted', hw_df['DATE'].cast('date')).select('DATE', 'hw_date_formatted')
            hw_df = hw_df.withColumn('hw_day_of_week', func.date_format('hw_date_formatted', 'u')).select('DATE', 'hw_date_formatted', 'hw_day_of_week')
            
            if hw_df.count() > 0:
                join_count_df = hw_df.join(aht_df, col('hw_date_formatted') == col('ht_date_formatted')).select('hw_date_formatted', 'hw_day_of_week', 'count').orderBy("hw_date_formatted", ascending=True)
                
                if join_count_df.count() > 0:
                    if forecast_day_of_week < 5:
                        join_final_df = join_count_df.filter(func.col('hw_day_of_week') < 5)
                    else:
                        join_final_df = join_count_df.filter(func.col('hw_day_of_week') >= 5)
                        
                    if forecast_rain == 0:
                        join_final_df = join_final_df.orderBy("hw_date_formatted", ascending=False).limit(5)
                    
                    if join_final_df.count() > 0:
                        aggregate_count = join_final_df.groupBy().agg(func.avg(func.col("count"))).collect()[0][0]
                        aggregate_count = math.ceil(aggregate_count)
                        print("     aggregate_count: " + str(aggregate_count))
                        result_list.append([str(forecast_date), str(forecast_rain), str(aggregate_count)])
                        print("     result_list: " + str(result_list))
                    else:
                        print("     join_final_df.count() is 0")
                else:
                    print("     join_count_df.count() is 0")
            else:
                print("     hw_df.count() after date format is 0")
        else:
            print("     hw_df.count() main is 0")
    else:
        print("     forecast_date is not greater than today")
        
with open('predicted_user_demand.csv', 'w', newline='') as f:
    wr = csv.writer(f)
    wr.writerows(result_list)

print ("User demand for taxi booking predicted successfully")

spark_session.stop()
