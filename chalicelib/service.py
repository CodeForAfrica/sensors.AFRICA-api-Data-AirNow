import boto3
import json
import pickle
import requests

from chalicelib.sensorafrica import (
    get_sensors_africa_nodes,
    post_sensor_data, )

from chalicelib.settings import S3_BUCKET_NAME, S3_OBJECT_KEY, S3_HISTORY_OBJECT_KEY, AIRNOW_API_KEY, OWNER_ID, LAST_DATA_RECORDED_ON

from time import localtime, sleep, strftime
from datetime import datetime as dt, timedelta

def get_nodes_sensor_data(day):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.138 Safari/537.36",
        }
    response = requests.get(
        url="https://www.airnowapi.org/aq/data/?startDate={}T00&endDate={}T23&parameters=PM25,PM10&BBOX=-18.388062,-38.456014,52.979126,38.640199&dataType=C&format=application/json&verbose=1&nowcastonly=0&includerawconcentrations=1&API_KEY={}".format(day, day, AIRNOW_API_KEY), headers=headers)
    if not response.ok:
        raise Exception(response.reason)
    return response.json()


def run(app):
    nodes = get_sensors_africa_nodes()
    nodes = [ node.get("uid") for node in nodes ]
  
    s3client = boto3.client("s3", region_name="eu-west-1")
    try:
        response = s3client.get_object(Bucket=S3_BUCKET_NAME, Key=S3_OBJECT_KEY)
        body = response['Body'].read()
        node_last_entry_dict = pickle.loads(body)
    except:
        node_last_entry_dict = dict()


    today = strftime("%Y-%m-%d", localtime())

    data = get_nodes_sensor_data(today)
    africa_sensor_data = [ d for d in data if d["FullAQSCode"] in nodes ]

    for sensor_data in africa_sensor_data:
        if not sensor_data["FullAQSCode"] in node_last_entry_dict:
                node_last_entry_dict[sensor_data["FullAQSCode"]] = "2000-01-01T00:00"
        
        last_entry = node_last_entry_dict[sensor_data["FullAQSCode"]]

        if dt.strptime(sensor_data["UTC"], "%Y-%m-%dT%H:%M") > dt.strptime(last_entry, "%Y-%m-%dT%H:%M"):
            value_type = "P2"
            if sensor_data["Parameter"] != "PM2.5":
                value_type = "P1"

            sensor_data_values = [{
                            "value": sensor_data["Value"],
                            "value_type": value_type
                        }]

            post_sensor_data({ 
                        "sensordatavalues": sensor_data_values, 
                        "timestamp": sensor_data["UTC"]
                        }, sensor_data["FullAQSCode"], "-")
                
                #update pickle variable               
            node_last_entry_dict[sensor_data["FullAQSCode"]] = sensor_data["UTC"]
            s3client.put_object(Body=pickle.dumps(node_last_entry_dict), Bucket=S3_BUCKET_NAME, Key=S3_OBJECT_KEY)
   

def history(app):
    nodes = get_sensors_africa_nodes()
    nodes = [ node.get("uid") for node in nodes ]
  
    s3client = boto3.client("s3", region_name="eu-west-1")
    try:
        response = s3client.get_object(Bucket=S3_BUCKET_NAME, Key=S3_HISTORY_OBJECT_KEY)
        body = response['Body'].read()
        last_date_dict = pickle.loads(body)
    except:
        last_date_dict = dict()


    day = strftime("%Y-%m-%d", localtime())

    data = get_nodes_sensor_data(today)
    africa_sensor_data = [ d for d in data if d["FullAQSCode"] in nodes ]

    for sensor_data in africa_sensor_data:
        if not sensor_data["FullAQSCode"] in node_last_entry_dict:
                node_last_entry_dict[sensor_data["FullAQSCode"]] = "2000-01-01T00:00"
        
        last_entry = node_last_entry_dict[sensor_data["FullAQSCode"]]

        if dt.strptime(sensor_data["UTC"], "%Y-%m-%dT%H:%M") > dt.strptime(last_entry, "%Y-%m-%dT%H:%M"):
            value_type = "P2"
            if sensor_data["Parameter"] != "PM2.5":
                value_type = "P1"

            sensor_data_values = [{
                            "value": sensor_data["Value"],
                            "value_type": value_type
                        }]

            post_sensor_data({ 
                        "sensordatavalues": sensor_data_values, 
                        "timestamp": sensor_data["UTC"]
                        }, sensor_data["FullAQSCode"], "-")
                
                #update pickle variable               
            node_last_entry_dict[sensor_data["FullAQSCode"]] = sensor_data["UTC"]
            s3client.put_object(Body=pickle.dumps(node_last_entry_dict), Bucket=S3_BUCKET_NAME, Key=S3_OBJECT_KEY)