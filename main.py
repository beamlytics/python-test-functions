from faker import Faker
import random
from faker.providers import BaseProvider
import uuid
import datetime
import pandas as pd
import json


df = pd.read_csv("Amazon-Products.csv")


f = Faker(["en_UK"])



class StreamProvider(BaseProvider):


    def clickstream_data(self):
        random_index = random.randint(0, len(df) - 1)
        event=random.choice(["view_item", "add-to-cart", "purchase","other"])

        return {
            "uid": random.randint(1, 100),
            "sessionId": random.choice(["null",str(uuid.uuid4())]),
            "returning": random.choice([True, False]),
            "lat": round(39.669082 + random.uniform(-0.005, 0.005),6),
            "lng": round(-80.312306 + random.uniform(-0.005, 0.005),6),
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
            "timestamp":int((datetime.datetime.strptime(f.date("%Y-%m-%d"), "%Y-%m-%d")
                + datetime.timedelta(hours=random.randint(0, 23), minutes=random.randint(0, 59), seconds=random.randint(0, 59)))
                .timestamp() * 1000),
            "ecommerce": {
                "items": [{
                    "item_name": df["item_name"].iloc[random_index],
                    "item_id": random_index,  # Generate random item ID (5 digits)
                    "price": df["price"].iloc[random_index],
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
            "client_id": random.randint(52393559, 52393559 + 99),  # Random client ID from a pool of 500-600
            "page_previous": random.choice(["null",f"P_{random.randint(1, 100)}"]),  # Random previous page (P1-P10)
            "page": random.choice([f"P_{random.randint(1, 100)}"]),
            "event_datetime": str(datetime.datetime.strptime(f.date("%Y-%m-%d"), "%Y-%m-%d"))
        }
    def transactionschema_data(self):
        random_index = random.randint(0, len(df) - 1)

        return{
            "order_number": random.choice([str(uuid.uuid4())]),
            "user_id": random.randint(74378, 74378 + 599),
            "store_id":random.randint(1,50),
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
            "timestamp": str(int((datetime.datetime.strptime(f.date("%Y-%m-%d"), "%Y-%m-%d")+ datetime.timedelta(hours=random.randint(0, 23), minutes=random.randint(0, 59),
                        seconds=random.randint(0, 59))).timestamp() * 1000)),

            "ecommerce": {
                    "items": [{
                        "item_name": df["item_name"].iloc[random_index],
                        "item_id": random_index,  # Generate random item ID (5 digits)
                        "price": df["price"].iloc[random_index],
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
            "event_datetime": str(datetime.datetime.strptime(f.date("%Y-%m-%d"), "%Y-%m-%d"))
    }

    def inventoryschema_data(self):
        random_index = random.randint(0, len(df) - 1)
        return{
            "count": random.randint(1, 100),
            "sku": 10050 + random.randint(1, 1000),
            "aisleID": random.randint(1, 100),
            "product_name_choices": df["item_category_2"].iloc[random_index],
            "departmentId": random.randint(1, 100),
            "price": df["price"].iloc[random_index],
            "recipeID": "Null",
            "image": df["image"].iloc[random_index],
            "timestamp": str(int((datetime.datetime.strptime(f.date("%Y-%m-%d"), "%Y-%m-%d") + datetime.timedelta(
                          hours=random.randint(0, 23), minutes=random.randint(0, 59),
                          seconds=random.randint(0, 59))).timestamp() * 1000)),
            "store_id": random.randint(1, 10),
            "prodduct_id": 10050 + random.randint(1, 1000)




        }

def write_dict_to_json(data, filename):
    with open(filename, 'w') as f:
        json.dump(data, f, indent=4)



for i in range (0,2):
    f.add_provider(StreamProvider)
    clickstream_schema=f.clickstream_data()
    transaction_schema=f.transactionschema_data()
    inventory_schema= f.inventoryschema_data()
    print(clickstream_schema)
    print()
    print(transaction_schema)
    print()
    print(inventory_schema)

    write_dict_to_json(inventory_schema, f"inventory_schema_{i+1}.json")
    write_dict_to_json(clickstream_schema, f"clickstream_schema_{i+1}.json")
    write_dict_to_json(transaction_schema, f"transaction_schema_{i+1}.json")
