#### Forecast Easy Get MEAN of last hour (12*1=12 samples) Moving Average 12 samples

from time import time
import psycopg2
import numpy as np
i = 1;

#while True
params = {'database': '***',
          'user': '****',
          'password': '****',
          'host': '****',
          'port': 1111
         }


# conn = psycopg2.connect(**params)
# cursor = conn.cursor()
# query = '''SELECT id FROM cenacepower.webg ORDER BY timestamp DESC LIMIT 1'''
# postgreSQL_select_Query = query
# cursor.execute(postgreSQL_select_Query)
# index = cursor.fetchone()
# cursor.close()
# conn.close()
# index = index[0]








try:
        conn = psycopg2.connect(**params)
        cursor = conn.cursor()
        #print("Database connected")
        query = '''SELECT id,"demandaNAC2", timestamp FROM cenacepower.webg ORDER BY timestamp DESC LIMIT 12'''
        #query = '''SELECT timestamp, id,"demandaNAC2","enlaceNAC" FROM cenacepower.webg WHERE id={}'''.format( str(i))


        #print(query)
        postgreSQL_select_Query = query
        cursor.execute(postgreSQL_select_Query)
        payload = cursor.fetchall()
        cursor.close()
        conn.close()       

except (Exception, psycopg2.Error) as error:
    print("Error while fetching data from PostgreSQL\n", error)
    

data= []
adjust = 0;


for i in range(0, len(payload)):
    data.append(int(payload[i][1]))
    


forecast = round(np.average(data))
timestamp = round(payload[1][2])



for i in range(payload[0][0]+1,  payload[0][0]+13):
    print(i)
    try:
            
            adjust = adjust + 300
            
            conn = psycopg2.connect(**params)
            cursor = conn.cursor()
            insert = '''INSERT INTO cenacepower.forecast (id, timestamp,forecasted) 
            VALUES ({}, {} ,{});'''.format(i, timestamp + adjust, forecast)
            cursor.execute(insert)
            conn.commit()
            cursor.close()
            conn.close()       

    except (Exception, psycopg2.Error) as error:
        print("Error while sending data to PostgreSQL\n", error)
    