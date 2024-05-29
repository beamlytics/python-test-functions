import json
import random
import time as tme
import uuid
from datetime import *

from google.auth import compute_engine

import google.auth
import pandas as pd
from faker import Faker
from faker.providers import BaseProvider
from google.cloud import pubsub_v1

df = pd.read_csv("Amazon-Products.csv")


f = Faker(["en_US"])
#locale.setlocale(locale.LC_NUMERIC, 'en_IN.UTF-8')

# Replace with your project ID
#
# Replace with your desired topic name
#topic_id = "python-test-functions"



class StreamProvider(BaseProvider):


    def clickstream_data(self, event):
        random_index = random.randint(0, len(df) - 1)
        ##event=random.choice(["view_item", "add-to-cart", "purchase","other","browse"])
        latitude_values= [
               40.7128, 35.6895, 51.5074, 48.8566, 39.9042, 55.7558, 41.0082, 25.2769, -22.9068, -33.8688, 
               30.0444, 41.9028, 34.0522, 19.0760, -34.6037, 37.5665, 31.2304, 19.4326, 43.6511, 13.7563, 
               52.5200, -6.2088, -12.0464, 40.4168, 55.7558, 48.2082, 52.3676, 37.9838, 50.8503, 47.4979, 
               55.6761, 53.3498, 60.1695, 38.7223, 59.9139, 50.0755, 24.7136, 1.3521, 59.3293, 52.2297, 
               47.3769, 49.2827, 41.3851, 43.7696, 45.4408, 50.0755, 43.7696, 49.2827, -33.9249, 22.3193
            ]
        longitude_values= [
             -74.0060, 139.6917, -0.1278, 2.3522, 116.4074, 37.6173, 28.9784, 55.2962, -43.1729, 151.2093, 
              31.2357, 12.4964, -118.2437, 72.8777, -58.3816, 126.9780, 121.4737, -99.1332, -79.3832, 100.5018, 
              13.4050, 106.8456, -77.0428, -3.7038, 37.6173, 16.3738, 4.9041, 23.7275, 4.3517, 19.0402, 
              12.5683, -6.2603, 24.9354, -9.1393, 10.7522, 14.4378, 46.6753, 103.8198, 18.0686, 21.0122, 
               8.5417, -123.1207, 2.1734, 11.2558, 12.3155, 14.4378, 11.2558, -123.1207, 18.4241, 114.1694
             ]
        random_loc= random.randint(0,len(latitude_values)-1)

        return{
            "uid": random.randint(1, 100),
            "sessionId": random.choice([str(uuid.uuid4())]),
            "returning": random.choice([True, False]),
            "lat": latitude_values[random_loc],# TODO Note down 10 cities 
            "lng": longitude_values[random_loc],# TODO Note down 10 cities 
            "agent": random.choice(["Mozilla/5.0 (Windows Phone 10.0; Android 6.0.1; Microsoft; RM-1152) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/52.0.2743.116 Mobile Safari/537.36 Edge/15.15254",
                                    "Mozilla/5.0 (iPhone9,4; U; CPU iPhone OS 10_0_1 like Mac OS X) AppleWebKit/602.1.50 (KHTML, like Gecko) Version/10.0 Mobile/14A403 Safari/602.1",
                                    "Mozilla/5.0 (iPhone14,3; U; CPU iPhone OS 15_0 like Mac OS X) AppleWebKit/602.1.50 (KHTML, like Gecko) Version/10.0 Mobile/19A346 Safari/602.1",
                                    "Mozilla/5.0 (Linux; Android 12; Redmi Note 9 Pro) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Mobile Safari/537.36",
                                    "Mozilla/5.0 (Linux; Android 12; moto g pure) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Mobile Safari/537.36",
                                    "Mozilla/5.0 (Linux; Android 13; Pixel 6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Mobile Safari/537.36",
                                    "Mozilla/5.0 (Linux; Android 13; SM-S901B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Mobile Safari/537.36",
                                    "Mozilla/5.0 (iPad; CPU OS 12_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148"]),
            "event": event,
            "transaction": event =="purchase",# False for no purchase
            #"timestamp":int((datetime.datetime.strptime(f.date("%Y-%m-%d"), "%Y-%m-%d")
            #    + datetime.timedelta(hours=random.randint(0, 23), minutes=random.randint(0, 59), seconds=random.randint(0, 59)))
            #    .timestamp() * 1000),
            "timestamp": int((tme.time())*1000),
            "ecommerce": {
                "items": [{
                    "item_name": df["item_name"].iloc[random_index],
                    "item_id": str(random_index),  # Generate random item ID (5 digits)
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
                    "quantity": random.randint(1, 3)  # Random quantity between 1 and   3
                }]
            },
            "user_id": random.randint(74378, 74378 + 599),  # Random user ID from a pool of 600
            "client_id": str(random.randint(52393559, 52393559 + 99)),  # Random client ID from a pool of 500-600
            "page_previous": random.choice(["null",f"P_{random.randint(1, 10)}"]),  # Random previous page (P1-P10)
            "page": random.choice([f"P_{random.randint(1, 10)}"]),
            #"event_datetime":(int(datetime.datetime.strptime("2023-11-21", "%Y-%m-%d").timestamp())*1000)
            "event_datetime": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
    
    
f.add_provider(StreamProvider)
publisher = pubsub_v1.PublisherClient()
project_id = "retail-pipeline-beamlytics"
#credentials, project = google.auth.default()



def get_data(event):
    """Fetches data based on the specified data type."""
    return f.clickstream_data(event)
   
def publish_message(publisher, topic_path, data):
    """Publishes a message to the specified topic."""
    data_str=data
    data_bytes = json.dumps(data_str).encode("utf-8")
    future = publisher.publish(topic_path, data_bytes)
    print(future.result())
    return future.result()  # Wait for the message to be published

def my_func(topic_id,event, number):
    """Main loop that publishes data to Pub/Sub, receiving the data type as an argument."""
    try:
        for i in range(0,number):
          data = get_data(event)
          print (data)
          print(publisher)
          print(topic_path)
          publish_message(publisher, topic_path, data)
          print(f"Published {topic_id} data to {topic_path}.")
    except Exception as e:
        print(f"Error publishing message: {e}")


topic_id = "Clickstream-inbound"
#topic_id = random.choice(["Clickstream-inbound", "Inventory-inbound", "Transactions-inbound">
topic_path = publisher.topic_path(project_id, topic_id)

##generate a browse event
my_func(topic_id,"browse", 1)