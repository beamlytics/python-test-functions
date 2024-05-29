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
import generate_clickstream
from time import sleep


df = pd.read_csv("Amazon-Products.csv")
publisher = pubsub_v1.PublisherClient()
project_id = "retail-pipeline-beamlytics"






def browse_cart_purchase(topic_id,event, number):
    """Main loop that publishes data to Pub/Sub, receiving the data type as an argument."""
    event_og = event
    try:
        for i in range(number):
            browse_data = generate_clickstream.get_data("browse")
            print (f"Browsing_time {i}.", browse_data)
            generate_clickstream.publish_message(publisher, topic_path, browse_data) 
            #print(publisher)
            #print(topic_path)
            print(f"Published {topic_id} data to {topic_path}.")
            sleep(2)
            

        browse_page = browse_data["page"]
        browse_data["event"] = "add to cart"
        browse_data["page_previous"] = browse_page
        browse_data["page"] = random.choice([f"P_{random.randint(1, 10)}"])
        cart_page = browse_data["page"]
        print("Add to cart event",browse_data)
        generate_clickstream.publish_message(publisher, topic_path, browse_data) 
        print(f"Published {topic_id} data to {topic_path}.")
        sleep(5)

    
        browse_data["event"] = "purchase"
        print("page_prev",cart_page)
        browse_data["transaction"] = True

        browse_data["page_previous"] = cart_page
        browse_data["page"] = random.choice([f"P_{random.randint(1, 10)}"])

        print("Purchase Event",browse_data)
        generate_clickstream.publish_message(publisher, topic_path, browse_data) 
        print(f"Published {topic_id} data to {topic_path}.")
        sleep(10)


    except Exception as e:
        print(f"Error publishing message: {e}")


topic_id = "Clickstream-inbound"
topic_path = publisher.topic_path(project_id, topic_id)
browse_cart_purchase(topic_id, "add to cart", 3)






        







        




