#!/usr/bin/env python
import csv
import math

result_list = []

with open('predicted_user_demand.csv') as csv_file:
    csv_reader = csv.reader(csv_file, delimiter=',')

    for row in csv_reader:
        forecast_date = row[0]
        forecast_rain = row[1]
        predicted_demand = row[2]
        estimate_resource_required = math.ceil(int(predicted_demand) / (30 * 60 * 24))
        result_list.append([str(forecast_date), str(forecast_rain), str(predicted_demand), str(estimate_resource_required)])
        print("result_list: " + str(result_list))

with open('calculated_resources.csv', 'w', newline='') as f:
    wr = csv.writer(f)
    wr.writerows(result_list)
    
print ("Resource count calculated successfully")
