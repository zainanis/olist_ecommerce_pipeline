
import pandas as pd
from datetime import datetime
from confluent_kafka import Producer
import time

orders = pd.read_csv("dataset/dynamic/olist_orders_dataset.csv")
items = pd.read_csv("dataset/dynamic/olist_order_items_dataset.csv")
payments =pd.read_csv("dataset/dynamic/olist_order_payments_dataset.csv")
reviews = pd.read_csv("dataset/dynamic/olist_order_reviews_dataset.csv")

conf = {
    'bootstrap.servers': 'localhost:9092'  
}

producer = Producer(conf)


order_ids = orders["order_id"].tolist()
batch_size = 2000


for i in range(0, len(order_ids), batch_size):
    

    batch_order_ids = order_ids[i:i+batch_size]
    
    # Filter batch
    batch_orders = orders[orders["order_id"].isin(batch_order_ids)]
    batch_items = items[items["order_id"].isin(batch_order_ids)]
    batch_payments = payments[payments["order_id"].isin(batch_order_ids)]
    batch_reviews = reviews[reviews["order_id"].isin(batch_order_ids)]
    
    # Convert to CSV string
    json_orders = batch_orders.to_json(orient="records")
    json_items = batch_items.to_json(orient="records")
    json_payments = batch_payments.to_json(orient="records")
    json_reviews = batch_reviews.to_json(orient="records")
    print(json_orders)

    producer.produce('orders',key=f'batch_{i//batch_size +1}',value=json_orders)
    producer.produce('order_items',key=f'batch_{i//batch_size +1}',value=json_items)
    producer.produce('order_payments',key=f'batch_{i//batch_size +1}',value=json_payments)
    producer.produce('order_reviews',key=f'batch_{i//batch_size +1}',value=json_reviews)

    producer.flush() 
    print(f"Batch {i//batch_size + 1}: sent {len(batch_order_ids)} orders to Kafka topics")
    time.sleep(60)
