import requests
from time import sleep
import random
import json
from sqlalchemy import text
from datetime import date, datetime
from aws_db_connector import AWSDBConnector


random.seed(100)
new_connector = AWSDBConnector()

def json_serial(obj):
    '''
    This function serializes objects not seriliazable by default json code.

    Args:
        obj: Object to serialize

    Returns:
        str: ISO formatted date string if obj is datetime or data.

    Raises:
        TypeError: If the obj is not serializable.

    '''
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError ("Type %s not serializable" % type(obj))

def send_to_kafka(topic, payload):
    '''
    This function sends data to the specified Kafka topic

    Args:
        topic (str): Kafka topic to send data  to.
        payload (str): JSON string payload to send.
     
    '''
    invoke_url = f"https://dob1hsyi8a.execute-api.us-east-1.amazonaws.com/first/topics/{topic}"
    headers = {'Content-Type': 'application/vnd.kafka.json.v2+json'}
    try:
        response = requests.post(invoke_url, data=payload, headers=headers)
        response.raise_for_status() 
        print(f"Sent to Kafka topic {topic}: {response.status_code} - {response.text}")
    except requests.exceptions.RequestException as e:
        print(f"Error sending to Kafka topic {topic}: {e}")

def fetch_send_data(connection, table_name, topic):
    '''
    This function fetches a random row of data from specified table and sends it to a Kafka topic. 

    Args: 
        connection: Database connection (SQLAlchemy engine).
        table_name (str): Name of the table to fetch data from.
        topic (str): Kafka topic to send data to.

    '''
    random_row = random.randint(0, 11000)
    query = text(f"SELECT * FROM {table_name} LIMIT {random_row}, 1")
    result = connection.execute(query)

    for row in result:
        row_result = dict(row._mapping)
        payload = json.dumps({
            "records": [
                {"value": row_result}
            ]
        }, default=json_serial)
        send_to_kafka(topic, payload)

def run_infinite_post_data_loop():
    '''
    This function runs on infinite loop to fetch data from the database and send it to Kafka. 
    
    '''
    engine = new_connector.create_db_connector()
    while True:
        try:
            sleep(random.randrange(0, 2))
            with engine.connect() as connection:
                fetch_send_data(connection, 'pinterest_data',"0affc011d3cf.pin" )
                fetch_send_data(connection, 'geolocation_data',"0affc011d3cf.geo" )
                fetch_send_data(connection, 'user_data',"0affc011d3cf.user" )

        except Exception as e:
            print(f"Error in main loop: {e}")
                
if __name__ == "__main__":
    run_infinite_post_data_loop()
    print('Working')


