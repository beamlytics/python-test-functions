import generate_clickstream
from google.cloud import pubsub_v1
from time import sleep
import random



publisher = pubsub_v1.PublisherClient()
project_id = "retail-pipeline-beamlytics"

topic_id = "Clickstream-inbound"
#topic_id = random.choice(["Clickstream-inbound", "Inventory-inbound", "Transactions-inbound">
topic_path = publisher.topic_path(project_id, topic_id)

def my_func(topic_id,event, number):
    """Main loop that publishes data to Pub/Sub, receiving the data type as an argument."""
    browse_user=random.randint(1, 100)
    for i in range(0,number):
        data = generate_clickstream.get_data("browse")
        data["uid"] = browse_user
        print (data)
        print(publisher)
        print(topic_path)
        generate_clickstream.publish_message(publisher, topic_path, data)
        print(f"Published {topic_id} data to {topic_path}.")
        sleep(5)


my_func(topic_id,"browse", 5)
