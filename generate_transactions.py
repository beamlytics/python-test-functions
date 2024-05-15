import json
import random
import time as tme
import uuid
from datetime import *

import google.auth
import pandas as pd
from faker import Faker
from faker.providers import BaseProvider
from google.cloud import pubsub_v1

df = pd.read_csv("Amazon-Products.csv")


f = Faker(["en_US"])



class StreamProvider(BaseProvider):

    def transactionschema_data(self):
        random_index = random.randint(0, len(df) - 1)

        return{
            "order_number": random.choice([str(uuid.uuid4())]),
            "user_id": random.randint(74378, 74378 + 599),
            "store_id":random.randint(1,10),
            "returning": random.choice([True, False]),
            "time_of_sale":random.randint(0,24),
            "department_id":random.randint(1,50),
            "product_id":random.randint(1,50),
            "product_count":random.randint(1,10),
            "price":random.randint(1,5000),
            "order_id": random.choice([str(uuid.uuid4())]),
            "order_dow": random.randint(1,7),
            "order_hour_of_day":random.randint(0,24),
            "order_woy":random.randint(1,52),
            "product_name": df["item_name"].iloc[random_index],
            "product_sku":random.randint(0,52),
            "image":df["image"].iloc[random_index],
            "timestamp": int((tme.time())*1000),

            "ecommerce": {
                    "items": [{
                        "item_name": df["item_name"].iloc[random_index],
                        "item_id": random_index,  # Generate random item ID (5 digits)
                        "price": float((df["price"].iloc[random_index])[1:].replace(',','')),
                        "item_brand": "Amazon",  # Fixed brand
                        "item_category": df["item_category"].iloc[random_index],
                        "item_category_2": df["item_category_2"].iloc[random_index],
                        "item_category_3": df["item_category_3"].iloc[random_index],
                        "item_category_4": df["item_category_4"].iloc[random_index],
                        "item_variant": random.choice(["Black", "White", "Blue"]),  # Random variant
                        "item_list_name": df["item_name"].iloc[random_index],
                        "item_list_id": "SR"+str(random_index),
                        "index": random_index,
                        "quantity": random.randint(1, 3)
                    }]
            },
            "client_id": random.randint(52393559, 52393559 + 99),
            "page_previous": random.choice(["null", f"P_{random.randint(1, 100)}"]),
            "page": random.choice([f"P_{random.randint(1, 100)}"]),
            "event_datetime": datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            
    }

f.add_provider(StreamProvider)
publisher = pubsub_v1.PublisherClient()
project_id = "retail-pipeline-beamlytics"
#credentials, project = google.auth.default()



def get_data():
    """Fetches data based on the specified data type."""
    return f.transactionschema_data()

def publish_message(publisher, topic_path, data):
    """Publishes a message to the specified topic."""
    data_str=data
    data_bytes = json.dumps(data_str).encode("utf-8")
    future = publisher.publish(topic_path, data_bytes)
    print(future.result())
    return future.result()  # Wait for the message to be published

def my_func(topic_id, number):
    """Main loop that publishes data to Pub/Sub, receiving the data type as an argument."""
    try:
        for i in range(0,number):
          data = get_data()
          print (data)
          print(publisher)
          print(topic_path)
          publish_message(publisher, topic_path, data)
          print(f"Published {topic_id} data to {topic_path}.")
    except Exception as e:
        print(f"Error publishing message: {e}")


topic_id = "Transactions-inbound"
#topic_id = random.choice(["Clickstream-inbound", "Inventory-inbound", "Transactions-inbound">
topic_path = publisher.topic_path(project_id, topic_id)

##generate a browse event
my_func(topic_id, 1)