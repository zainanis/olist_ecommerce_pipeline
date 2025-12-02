from confluent_kafka import Consumer
import boto3
import json
from datetime import datetime
import pandas as pd

credentials=pd.read_csv('IAM Access Keys.csv')

AWS_S3_BUCKET_NAME='olist--datalake'
AWS_REGION='ap-south-1'
AWS_ACCESS_KEY = credentials["Access key ID"].iloc[0]
AWS_SECRET_KEY = credentials["Secret access key"].iloc[0]


try:
    s3_client = boto3.client(
        service_name="s3",
        region_name=AWS_REGION,
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY
    )
except Exception as e:
    print("Failed to create S3 client:", e)
    raise SystemExit


conf= {
    'bootstrap.servers':'localhost:9092',
    'group.id':'order-consumer-group',
    'auto.offset.reset':'earliest'
}

consumer= Consumer(conf)

topics=['orders','order_items','order_payments','order_reviews']
consumer.subscribe(topics)

try:
    while True:
        msg=consumer.poll(1)
        if msg is None:
            continue
        if msg.error():
            raise Exception(msg.error())
        
        folder_name=msg.topic()
        batch=msg.key().decode('utf-8')
        current_time=datetime.now()
        year=current_time.year
        month=current_time.month
        day=current_time.day
        hour=current_time.hour

        path=f'dynamic/{folder_name}/{year}/{month}/{day}/{hour}/{batch}.json'

        s3_client.put_object(
            Bucket=AWS_S3_BUCKET_NAME,
            Key=path,
            Body=msg.value().decode('utf-8')
        )
        print(f'Batch: {batch} Topic: {folder_name} stored at path: {path}')
        
except KeyboardInterrupt:
    print("\nClosing consumer...")
finally:
    consumer.close()
        