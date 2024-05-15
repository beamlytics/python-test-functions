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

    def inventoryschema_data(self):
        random_index = random.randint(0, len(df) - 1)
        return{
            "count": random.randint(1, 100),
            "sku": 10050 + random.randint(1, 1000),
            "aisleID": random.randint(1, 100),
            "product_name": df["item_category_2"].iloc[random_index],
            "departmentId": random.randint(1, 100),
            "price": float((df["price"].iloc[random_index])[1:].replace(',','')),
            "recipeID": "Null",
            "image": df["image"].iloc[random_index],
            "timestamp": int((tme.time())*1000),
            "store_id": random.randint(1, 10),
            "product_id": 10050 + random.randint(1, 1000)

        }
    
f.add_provider(StreamProvider)
publisher = pubsub_v1.PublisherClient()
project_id = "retail-pipeline-beamlytics"
#credentials, project = google.auth.default()



def get_data():
    """Fetches data based on the specified data type."""
    return f.inventoryschema_data()

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


topic_id = "Inventory-inbound"
#topic_id = random.choice(["Clickstream-inbound", "Inventory-inbound", "Transactions-inbound">
topic_path = publisher.topic_path(project_id, topic_id)

##generate a browse event
my_func(topic_id, 1)