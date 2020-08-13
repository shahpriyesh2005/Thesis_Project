#!/usr/bin/env python
import pandas as pd
import json
import datetime
from pymongo import MongoClient
from pathlib import Path

file_path = Path("/home/priyesh/Documents/Thesis/Dataset/")
db_url = "localhost"
db_port = 27017
db_name = "thesisproject"
coll_name = "historical_weather_data"

mongoClient = MongoClient(db_url, db_port)
dbName = mongoClient[db_name]
collName = dbName[coll_name]
print ('Collection count at the start: ' + str(collName.count()))

# Import taxi data CSV file and load it in Mongo DB
def import_file(csv_file_name):
    fileName = file_path / csv_file_name
    print ('fileName: ' + str(fileName))
    
    # Read data, drop unwanted columns and insert in the database
    csvData = pd.read_csv(fileName)
    csvData = csvData.drop(['STATION', 'SOURCE', 'AWND', 'BackupDirection', 'BackupDistance', 'BackupDistanceUnit', 'BackupElements', 'BackupElevation', 'BackupElevationUnit', 'BackupEquipment', 'BackupLatitude', 'BackupLongitude', 'BackupName', 'CDSD', 'CLDD', 'DSNW', 'HDSD', 'HTDD', 'HeavyFog', 'HourlyAltimeterSetting', 'HourlyDewPointTemperature', 'HourlyDryBulbTemperature', 'HourlyPrecipitation', 'HourlyPresentWeatherType', 'HourlyPressureChange', 'HourlyPressureTendency', 'HourlyRelativeHumidity', 'HourlySeaLevelPressure', 'HourlySkyConditions', 'HourlyStationPressure', 'HourlyVisibility', 'HourlyWetBulbTemperature', 'HourlyWindDirection', 'HourlyWindGustSpeed', 'HourlyWindSpeed', 'MonthlyAverageRH', 'MonthlyDaysWithGT001Precip', 'MonthlyDaysWithGT010Precip', 'MonthlyDaysWithGT32Temp', 'MonthlyDaysWithGT90Temp', 'MonthlyDaysWithLT0Temp', 'MonthlyDaysWithLT32Temp', 'MonthlyDepartureFromNormalAverageTemperature', 'MonthlyDepartureFromNormalCoolingDegreeDays', 'MonthlyDepartureFromNormalHeatingDegreeDays', 'MonthlyDepartureFromNormalMaximumTemperature', 'MonthlyDepartureFromNormalMinimumTemperature', 'MonthlyDepartureFromNormalPrecipitation', 'MonthlyDewpointTemperature', 'MonthlyGreatestPrecip', 'MonthlyGreatestPrecipDate', 'MonthlyGreatestSnowDepth', 'MonthlyGreatestSnowDepthDate', 'MonthlyGreatestSnowfall', 'MonthlyGreatestSnowfallDate', 'MonthlyMaxSeaLevelPressureValue', 'MonthlyMaxSeaLevelPressureValueDate', 'MonthlyMaxSeaLevelPressureValueTime', 'MonthlyMaximumTemperature', 'MonthlyMeanTemperature', 'MonthlyMinSeaLevelPressureValue', 'MonthlyMinSeaLevelPressureValueDate', 'MonthlyMinSeaLevelPressureValueTime', 'MonthlyMinimumTemperature', 'MonthlySeaLevelPressure', 'MonthlyStationPressure', 'MonthlyTotalLiquidPrecipitation', 'MonthlyTotalSnowfall', 'MonthlyWetBulb', 'NormalsCoolingDegreeDay', 'NormalsHeatingDegreeDay', 'REM', 'REPORT_TYPE_1', 'SOURCE_1', 'ShortDurationEndDate005', 'ShortDurationEndDate010', 'ShortDurationEndDate015', 'ShortDurationEndDate020', 'ShortDurationEndDate030', 'ShortDurationEndDate045', 'ShortDurationEndDate060', 'ShortDurationEndDate080', 'ShortDurationEndDate100', 'ShortDurationEndDate120', 'ShortDurationEndDate150', 'ShortDurationEndDate180', 'ShortDurationPrecipitationValue005', 'ShortDurationPrecipitationValue010', 'ShortDurationPrecipitationValue015', 'ShortDurationPrecipitationValue020', 'ShortDurationPrecipitationValue030', 'ShortDurationPrecipitationValue045', 'ShortDurationPrecipitationValue060', 'ShortDurationPrecipitationValue080', 'ShortDurationPrecipitationValue100', 'ShortDurationPrecipitationValue120', 'ShortDurationPrecipitationValue150', 'ShortDurationPrecipitationValue180', 'TStorms', 'WindEquipmentChangeDate'], axis=1)
    csvData = csvData[csvData['REPORT_TYPE'].str.contains('SOD')]
    csvData = csvData.drop(['REPORT_TYPE', 'DailyAverageDewPointTemperature', 'DailyAverageDryBulbTemperature', 'DailyAverageRelativeHumidity', 'DailyAverageSeaLevelPressure', 'DailyAverageStationPressure', 'DailyAverageWetBulbTemperature', 'DailyAverageWindSpeed', 'DailyCoolingDegreeDays', 'DailyDepartureFromNormalAverageTemperature', 'DailyHeatingDegreeDays', 'DailyMaximumDryBulbTemperature', 'DailyMinimumDryBulbTemperature', 'DailyPeakWindDirection', 'DailyPeakWindSpeed', 'DailySnowDepth', 'DailySnowfall', 'DailySustainedWindDirection', 'DailySustainedWindSpeed', 'DailyWeather', 'Sunrise', 'Sunset'], axis=1)
    jsonData = json.loads(csvData.to_json(orient='records'))
    collName.insert_many(jsonData)

# Call import_file function with CSV file name
import_file("weather_2019.csv")

print ('Collection count at the end: ' + str(collName.count()))
print ("Historical weather data imported successfully")
