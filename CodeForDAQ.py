##FINAL SCRIPT
from bs4 import BeautifulSoup
import requests
import re
import edmodules
import glob
import re
import json
import time
from datetime import datetime
import pytz
import psycopg2
from sshtunnel import SSHTunnelForwarder
import numpy as np
import time
import requests

##########################
def send_data_web(timestamp,payload):
    try:
            params = {
                     'database': '****',
                     'user': '****',
                     'password': '****',
                     'host': '****',
                     'port': 1111
                     }


            conn = psycopg2.connect(**params)
            cursor = conn.cursor()
            print("Database connected")
                        
            columns = ""
            for key in payload:

                c = '"' +str(key).split('_')[1] + '",'
                columns = columns + c
                
            values= ""
            for value in payload.values():    
                v = str(value[1]) + ','

                values = values + v
            
            
            query = "INSERT INTO cenacepower.webg (timestamp"  + "," + columns[:-1] + ") VALUES (" + str(timestamp) + "," + values[:-1] + ")"
            print(query)
            postgreSQL_select_Query = query

            cursor.execute(postgreSQL_select_Query)
            conn.commit()
            count = cursor.rowcount


            print(count, "Record inserted successfully")
            if conn:
                cursor.close()
                conn.close()
            return '_No errors- PSQL'


    except (Exception, psycopg2.Error) as error:
        print("Error while fetching data from PostgreSQL\n", error)
        if conn:
                cursor.close()
                conn.close()
        return error
############# Script for all 

##FINAL SCRIPT
from bs4 import BeautifulSoup
import requests
import re
import edmodules
import glob
import re
import json
import time
from datetime import datetime
import pytz
import psycopg2
from sshtunnel import SSHTunnelForwarder
import numpy as np
import time
import requests

cookies = {
    'ASP.NET_SessionId': 'b2m11adxowxoduhv2dxnperq',
}

headers = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.9',
    'Cache-Control': 'max-age=0',
    'Connection': 'keep-alive',
    # 'Cookie': 'ASP.NET_SessionId=b2m11adxowxoduhv2dxnperq',
    'Sec-Fetch-Dest': 'document',
    'Sec-Fetch-Mode': 'navigate',
    'Sec-Fetch-Site': 'none',
    'Sec-Fetch-User': '?1',
    'Sec-GPC': '1',
    'Upgrade-Insecure-Requests': '1',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/105.0.0.0 Safari/537.36',
}

response = requests.get('https://www.cenace.gob.mx/GraficaDemanda.aspx', cookies=cookies, headers=headers)
soup = BeautifulSoup(response.text, 'html.parser').find("div", {"class": "columna-estado-sistema"})
result= soup.find_all("span",{'bold'})

from time import time
values = {}
timestamp = round(time())
values['time'] = timestamp
payload = {}

for item in result:
    key = item.attrs['id']
    
    div = soup.find(id=key)
    
    value = div.contents[0]
    value_raw = re.sub('[^\d]', '',div.contents[0])
    payload[key] = [value, value_raw]

values['data'] = payload

import json


with open(str('./SIN_WEB/') + str(timestamp)+ ".json", "w") as outfile:
        json.dump(values, outfile)
        
        


send_data_web(timestamp, payload)

#requests.post('https://api.telegram.org/sendMessage', 
#                  data={'chat_id': '-654475747', 'text':'SIN_CENACE_' + str(timestamp) + "PSQL OK"})