import psycopg2
i = 1;

#while True
params = {'database': '***',
          'user': '****',
          'password': '****',
          'host': '****',
          'port': 1111
         }


conn = psycopg2.connect(**params)
cursor = conn.cursor()
query = '''SELECT id FROM cenacepower.webg ORDER BY timestamp DESC LIMIT 1'''
postgreSQL_select_Query = query
cursor.execute(postgreSQL_select_Query)
index = cursor.fetchone()
cursor.close()
conn.close()
index = index[0]

for i in range(index - 15,index+15):
    print('Trying id = ',i)
    try:
            params = {'database': '***',
          'user': '****',
          'password': '****',
          'host': '****',
          'port': 1111
         }


            conn = psycopg2.connect(**params)
            cursor = conn.cursor()
            #print("Database connected")
            #query = '''SELECT timestamp, id,"demandaNAC2","enlaceNAC" FROM cenacepower.webg ORDER BY timestamp DESC LIMIT 1'''
            # query = '''SELECT timestamp, id,"demandaNAC2","enlaceNAC" FROM cenacepower.webg WHERE id={}'''.format( str(i))
            query = '''SELECT
                       timestamp,id,"demandaNAC2" ,
                        CASE
                            WHEN "enlaceNAC" <> 0 THEN "enlaceNAC"
                            ELSE (
                                SELECT
                                    "enlaceNAC"
                                FROM
                                    cenacepower.webg X
                                WHERE
                                    "enlaceNAC" <> 0 AND timestamp <= cenacepower.webg.timestamp
                                    ORDER BY
                                    timestamp DESC
                                LIMIT 1
                            )
                        END
                    FROM
                        cenacepower.webg
                    WHERE
                        id={}'''.format(i)
            
            
            #print(query)
            postgreSQL_select_Query = query
            cursor.execute(postgreSQL_select_Query)
            payload = cursor.fetchall()
            
            if payload == []:
                print("break at i=" , i)
                break
                
                
                
            payload = payload[0]
            
            timestamp = payload[0]
            idtable = payload[1]
            demanda = payload[2]
            pronostico = payload[3]
                        
            ####Add Files
            #query = '''SELECT timestamp, id,"demandaNAC2","enlaceNAC" FROM cenacepower.webg ORDER BY timestamp DESC LIMIT 1'''
            
            # postgreSQL_select_Query = query
            # cursor.execute(postgreSQL_select_Query)
            cursor.close()
            conn.close()       

    except (Exception, psycopg2.Error) as error:
        print("Error while fetching data from PostgreSQL\n", error)
        
    try:
            params = {'database': '***',
          'user': '****',
          'password': '****',
          'host': '****',
          'port': 1111
         }


            conn = psycopg2.connect(**params)
            cursor = conn.cursor()
            #print("Database connected")
            #query = '''SELECT timestamp, id,"demandaNAC2","enlaceNAC" FROM cenacepower.webg ORDER BY timestamp DESC LIMIT 1'''

            insert = '''UPDATE cenacepower.forecast 
            SET timestamp = {}, "demandaNAC2" = {}, "enlaceNAC" = {} 
            WHERE id = {};'''.format(timestamp, demanda, pronostico, idtable)
            cursor.execute(insert)
            conn.commit()

             

            cursor.close()
            conn.close()       

    except (Exception, psycopg2.Error) as error:
        print("Error while sending data to PostgreSQL\n", error)
     

            