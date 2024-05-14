import json
import locale
import time as tme
from datetime import *

#tme.init()

#print(locale.setlocale(locale.LC_NUMERIC, 'en_DK.UTF-8'))
data={}
data["event_datetime"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
data["timestamp"]  = int((tme.time())*1000)
price = '₹9,99,999.00'
data["price"] = float((price[1:]).replace(',',''))
print(data)
data_str = json.dumps(data)
# NOW = datetime.now()

# TODAY = date.today()

# print(NOW)
# print(TODAY)
# print(data_str)