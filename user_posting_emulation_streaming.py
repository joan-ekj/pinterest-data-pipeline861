import requests
from time import sleep
import random
import json
import sqlalchemy
from sqlalchemy import text
from datetime import datetime, date

random.seed(100)

class AWSDBConnector:
    def __init__(self):
        self.HOST = "pinterestdbreadonly.cq2e8zno855e.eu-west-1.rds.amazonaws.com"
        self.USER = 'project_user'
        self.PASSWORD = ':t%;yCY3Yjg'
        self.DATABASE = 'pinterest_data'
        self.PORT = 3306
        
    def create_db_connector(self):
        engine = sqlalchemy.create_engine(f"mysql+pymysql://{self.USER}:{self.PASSWORD}@{self.HOST}:{self.PORT}/{self.DATABASE}?charset=utf8mb4")
        return engine

new_connector = AWSDBConnector()

def json_serial(obj):
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError ("Type %s not serializable" % type(obj))

def send_to_kinesis(stream_name, payload):
    invoke_url = f"https://dob1hsyi8a.execute-api.us-east-1.amazonaws.com/second/streams/{stream_name}/record"
    headers = {'Content-Type': 'application/json'}
    try:
        response = requests.put(invoke_url, data=payload, headers=headers)
        response.raise_for_status() 
        print(f"Sent data to {stream_name}: {response.status_code} - {response.text}")
    except requests.exceptions.RequestException as e:
        print(f"Error sending to stream {stream_name}: {e}")

def run_infinite_post_data_loop():
    while True:
        sleep(random.randrange(0, 2))
        random_row = random.randint(0, 11000)
        engine = new_connector.create_db_connector()

        with engine.connect() as connection:

            
            pin_string = text(f"SELECT * FROM pinterest_data LIMIT {random_row}, 1")
            pin_selected_row = connection.execute(pin_string)
            
            for row in pin_selected_row:
                pin_result = dict(row._mapping)
                payload = json.dumps({
                            "StreamName": "streaming-0affc011d3cf-pin",
                            "Data": pin_result,
                            "PartitionKey": "stream-data"
                        }, default=json_serial)
                send_to_kinesis("streaming-0affc011d3cf-pin", payload)

        
            geo_string = text(f"SELECT * FROM geolocation_data LIMIT {random_row}, 1")
            geo_selected_row = connection.execute(geo_string)
            
            for row in geo_selected_row:
                geo_result = dict(row._mapping)
                payload = json.dumps({
                            "StreamName": "streaming-0affc011d3cf-geo",
                            "Data": geo_result,
                            "PartitionKey": "stream-data"
                        }, default=json_serial)
                send_to_kinesis("streaming-0affc011d3cf-geo", payload)
        
          
            user_string = text(f"SELECT * FROM user_data LIMIT {random_row}, 1")
            user_selected_row = connection.execute(user_string)
            
            for row in user_selected_row:
                user_result = dict(row._mapping)
                payload = json.dumps({
                            "StreamName": "streaming-0affc011d3cf-user",
                            "Data": user_result,
                            "PartitionKey": "stream-data"
                        }, default=json_serial)
                send_to_kinesis("streaming-0affc011d3cf-user", payload)
        
            print(pin_result)
            print(geo_result)
            print(user_result)

if __name__ == "__main__":
    run_infinite_post_data_loop()
    print('Working')