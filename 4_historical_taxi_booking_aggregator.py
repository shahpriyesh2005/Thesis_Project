#!/usr/bin/env python
from pymongo import MongoClient
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, countDistinct
from pyspark.sql.types import IntegerType
import pyspark.sql.functions as func

# Database details
db_url = "localhost"
db_port = 27017
db_name = "thesisproject"
coll_name = "historical_taxi_data"

mongoClient = MongoClient(db_url, db_port)
dbName = mongoClient[db_name]
collName = dbName[coll_name]

# Create a spark session to connect MongoDB using Spark-Mongo Connector
spark_session = SparkSession \
    .builder \
    .appName("myApp") \
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.11:2.4.1") \
    .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/") \
    .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/") \
    .getOrCreate()

# Load data from MongoDB collections to data frame
historical_taxi_df = spark_session.read.format("com.mongodb.spark.sql.DefaultSource").option("uri", "mongodb://localhost:27017/thesisproject.historical_taxi_data").load()
ht = historical_taxi_df.alias('ht')

# Cast the timestamp to date and aggregate the bookings for each date
ht_df = ht.withColumn('ht_date_formatted', ht['tpep_pickup_datetime'].cast('date')).groupBy('ht_date_formatted').count().select('ht_date_formatted', func.col('count').alias('count')).orderBy("ht_date_formatted", ascending=True)
ht_df.write.format("mongo").mode("append").option("database", "thesisproject").option("collection", "aggregated_historical_taxi_data").save()

print ("Historical taxi booking data aggregated successfully")

spark_session.stop()
