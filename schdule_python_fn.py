import argparse
import json
import locale
import time as tme
from datetime import *

import schedule as sch

#tme.init()

#print(locale.setlocale(locale.LC_NUMERIC, 'en_DK.UTF-8'))
def generate_browse_event(Event="browse"):
    data={}
    data["event"]=Event
    data["event_datetime"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    data["timestamp"]  = int((tme.time())*1000)
    price = (1000)
    #data["price"] = float((price[1:]).replace(',',''))
    data["price"] = price
    data_str = json.dumps(data)
    print("data is"+data_str)

def print_message():
    print("Task executed at:", time.strftime("%H:%M:%S"))    

# Initialize parser
parser = argparse.ArgumentParser()
 
# Adding optional argument
parser.add_argument("-o", "--Output", help = "Show Output")
parser.add_argument("-e", "--Event", required=True, help="pass one of these event names : browse,purchase")
parser.add_argument("-n", "--Number",type=int, default=2, help="pass how many messages need to be generated")
 
# Read arguments from command line
args = parser.parse_args()
 
if args.Output:
    print("Displaying Output as: % s" % args.Output)
if args.Event=='browse' and args.Number > 0:
    sch.every(args.Number).seconds.do(generate_browse_event,Event = args.Event ) 

while True:
    sch.run_pending()
    tme.sleep(1)