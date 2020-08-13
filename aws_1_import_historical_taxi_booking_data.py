#!/usr/bin/env python
import pandas as pd
import zipfile
import json
import datetime
from pymongo import MongoClient
from pathlib import Path

file_path = Path("/home/priyesh/Documents/Thesis/Dataset/")
db_url = "mongodb://admin:admin@ec2-54-81-206-27.compute-1.amazonaws.com"
db_port = 27017
db_name = "thesisproject"
coll_name = "historical_taxi_data"
read_chunk_size = 100000

mongoClient = MongoClient(db_url, db_port)
dbName = mongoClient[db_name]
collName = dbName[coll_name]
print ('Collection count at the start: ' + str(collName.count()))

# Import taxi data CSV file and load it in Mongo DB
def import_file(csv_file_name):
    csvFileName = file_path / csv_file_name
    print ('csvFileName: ' + str(csvFileName))
    
    counter = 0
    
    for csvData in pd.read_csv(csvFileName, chunksize=read_chunk_size, iterator=True):
        counter = counter + 1
        csvData = csvData.drop(['VendorID', 'tpep_dropoff_datetime', 'passenger_count', 'trip_distance', 'RatecodeID', 'store_and_fwd_flag', 'PULocationID', 'DOLocationID', 'payment_type', 'fare_amount', 'extra', 'mta_tax',  'tip_amount', 'tolls_amount', 'improvement_surcharge', 'total_amount', 'congestion_surcharge'], axis=1)
        
        if "green_tripdata_2019" in csv_file_name:
            csvData = csvData.drop(['ehail_fee', 'trip_type'], axis=1)
        
        jsonData = json.loads(csvData.to_json(orient='records'))
        collName.insert_many(jsonData)
        print ('Counter: ' + str(counter))

# Call import_file function with CSV file name
print ("Start Time ==> ", datetime.datetime.now())

import_file("yellow_tripdata_2019-01.csv")
import_file("yellow_tripdata_2019-02.csv")
import_file("yellow_tripdata_2019-03.csv")
import_file("yellow_tripdata_2019-04.csv")
import_file("yellow_tripdata_2019-05.csv")
import_file("yellow_tripdata_2019-06.csv")
import_file("yellow_tripdata_2019-07.csv")
import_file("yellow_tripdata_2019-08.csv")
import_file("yellow_tripdata_2019-09.csv")
import_file("yellow_tripdata_2019-10.csv")
import_file("yellow_tripdata_2019-11.csv")
import_file("yellow_tripdata_2019-12.csv")
import_file("green_tripdata_2019-01.csv")
import_file("green_tripdata_2019-02.csv")
import_file("green_tripdata_2019-03.csv")
import_file("green_tripdata_2019-04.csv")
import_file("green_tripdata_2019-05.csv")
import_file("green_tripdata_2019-06.csv")
import_file("green_tripdata_2019-07.csv")
import_file("green_tripdata_2019-08.csv")
import_file("green_tripdata_2019-09.csv")
import_file("green_tripdata_2019-10.csv")
import_file("green_tripdata_2019-11.csv")
import_file("green_tripdata_2019-12.csv")

print ('Collection count at the end: ' + str(collName.count()))
print ("End Time ==> ", datetime.datetime.now())
print ("Historical taxi booking data imported successfully")
