import argparse
import json
import locale
import time as tme
from datetime import *

import schedule as sch

import generate_clickstream
import generate_stock_event
import generate_transactions
import generate_purchase_event
import generate_air_gap

#tme.init()

#print(locale.setlocale(locale.LC_NUMERIC, 'en_DK.UTF-8'))
def generate_browse_event(event="browse"):
    # data={}
    # data["event"]=Event
    # data["event_datetime"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    # data["timestamp"]  = int((tme.time())*1000)
    # price = (1000)
    # #data["price"] = float((price[1:]).replace(',',''))
    # data["price"] = price
    # data_str = json.dumps(data)
    # print("data is"+data_str)
    topic_id = "Clickstream-inbound"
    generate_clickstream.my_func(topic_id,"browse", 1)

def generate_direct_purchase_event(Event="purchase only"):
    topic_id = "Clickstream-inbound"
    generate_clickstream.my_func(topic_id,"purchase", 1)

def generate_air_gap_event(Event="air_gap"):
    topic_id = "Clickstream-inbound"
    generate_air_gap.my_func(topic_id,"browse", 5)

def generate_browse_cart_purchase_event(Event= "purchase"):
    topic_id = "Clickstream-inbound"
    generate_purchase_event.browse_cart_purchase(topic_id, "purchase",1)

#Browse and add to cart only
def generate_browse_cart_purchase_event(Event= "add to cart"):
    topic_id = "Clickstream-inbound"
    generate_purchase_event.browse_cart_purchase(topic_id, "add to cart",1)

def generate_stockdata():
    topic_id = "Inventory-inbound"
    generate_stock_event.my_func(topic_id,1)

def generate_transactiondata():
    topic_id = "Transactions-inbound"
    generate_transactions.my_func(topic_id,1)




# Initialize parser
parser = argparse.ArgumentParser()
 
# Adding optional argument

parser.add_argument("-e", "--Event", required=True, help="pass one of these event names : browse,purchase")
parser.add_argument("-n", "--Number",type=int, default=2, help="pass how many messages need to be generated")
 
# Read arguments from command line
args = parser.parse_args()
 

if args.Event=='browse' and args.Number > 0:
    sch.every(args.Number).seconds.do(generate_browse_event,Event = args.Event ) 

if args.Event=='air_gap' and args.Number > 0:
    sch.every(args.Number).seconds.do(generate_air_gap_event,Event = args.Event ) 

if args.Event=='purchase' and args.Number > 0:
    sch.every(args.Number).seconds.do(generate_browse_cart_purchase_event,Event = args.Event ) 

if args.Event=='add to cart' and args.Number > 0:
    sch.every(args.Number).seconds.do(generate_browse_cart_purchase_event,Event = args.Event ) 

if args.Event == "stock" and args.Number > 0:
    sch.every(args.Number).seconds.do(generate_stockdata) 

if args.Event == "transaction" and args.Number > 0:
    sch.every(args.Number).seconds.do(generate_transactiondata) 

while True:
    sch.run_pending()
    tme.sleep(1)