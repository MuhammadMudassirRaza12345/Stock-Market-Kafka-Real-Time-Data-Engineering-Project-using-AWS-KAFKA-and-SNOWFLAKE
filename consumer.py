from bs4 import BeautifulSoup
import requests
import time
import datetime
import pandas as pd
import argparse
import json
from json import dumps,loads
from time import sleep
from kafka import KafkaConsumer 
import boto3
consumer = KafkaConsumer(
    'demo_testing2',
    bootstrap_servers=['localhost:9092'],  # add your IP here
    value_deserializer=lambda x: loads(x.decode('utf-8'))
    ) 

while True:
    try:
        for msg in consumer:
            cryptoRecord = msg.value
            transform_data = {
                        'SYSTEM_INSERTED_TIMESTAMP': cryptoRecord['SYSTEM_INSERTED_TIMESTAMP'], 
                        'RANK': int(cryptoRecord['RANK']),
                        'NAME': cryptoRecord['NAME'],
                        'SYMBOL': cryptoRecord['SYMBOL'],
                        'PRICE': float(cryptoRecord['PRICE'].replace('$', '').replace(',', '').replace(' ', '')),
                        'PERCENT_CHANGE_24H': float(cryptoRecord['PERCENT_CHANGE_24H'].replace('%', '').replace(',', '').replace(' ', '')),
                        'VOLUME_24H': float(cryptoRecord['VOLUME_24H'].replace('$', '').replace('B', 'E9').replace('M', 'E6').replace(',', '').replace(' ', '')),
                        'MARKET_CAP': float(cryptoRecord['MARKET_CAP'].replace('$', '').replace('B', 'E9').replace('M', 'E6').replace(',', '').replace(' ', '')),
                        'CURRENCY': 'USD'
                    }
            # Convert datetime to string format
            json_str = json.dumps(transform_data)
            # print(json_str)
            file_name = "real_time_data/top_100_crypto_data_" + str(cryptoRecord['SYSTEM_INSERTED_TIMESTAMP']) + '_' + str(cryptoRecord['RANK']) + '.json'
            s3 = boto3.client('s3', aws_access_key_id='AKIAXONTFZ3BXZON2AI2', aws_secret_access_key='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',region_name='us-east-1')
            response = s3.put_object(
                    Bucket='kafka-stock-market-video-mudassir',
                    Key=file_name,
                    Body=json_str)    
            print("file_uploaded sucessfully: ",file_name)
            #local file create
            # with open(file_name, 'w') as file:
            #     file.write(json_str)
            #     print(f"Data saved to {file_name}")

            # print(transform_data)
            sleep(1)
    except KeyboardInterrupt:
        break
consumer.close()