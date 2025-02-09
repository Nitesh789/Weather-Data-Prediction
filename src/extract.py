# Extract data from a public API

# transparency, wind_direction, wind_speed, temp2m

import requests
import json
import pandas as pd
from pandas.util import hash_pandas_object
import sqlalchemy as db
from sqlalchemy import create_engine, text 
from sqlalchemy import Table, Column, Integer, String, MetaData, select, insert

# response = requests.get("http://www.7timer.info/bin/api.pl?lon=113.17&lat=23.09&product=astro&output=json")
# response = response.json()
# print(json.dumps(response, indent=2))

# # [{"transparency": 2, "wind_direction": "NE", "wind_speed": 5, "temp2m": 18, "timepoint": 3},
# #  {},
# #  {}]v
# emp_list = []
# res = response["dataseries"]

# output = []
# for values in res:
#     record = {}
#     record["transparency"] = values["transparency"]
#     record["wind_direction"] = values["wind10m"]["direction"]
#     record["wind_speed"] = values["wind10m"]["speed"]
#     record["temp2m"] = values["temp2m"]
#     record["timepoint"] = values["timepoint"]
#     output.append(record)

# # print(output)

# df = pd.DataFrame(output)
# hs1=hash_pandas_object(df,index="False").tolist()
# hs1 = [str(value) for value in hs1]
# df = df.assign(city = "jaipur",hs1 = hs1)
# # print(df)


# server = "DESKTOP-QARI5IE\SQLEXPRESS"
# database = "projects"
# connection_string = (
#     f"mssql+pyodbc://@{server}/{database}"
#     "?driver=ODBC+Driver+18+for+SQL+Server"
#     "&Trusted_Connection=yes"
#     "&TrustServerCertificate=yes"
# )
# print(connection_string)
# engine = create_engine(connection_string)
# conn = engine.connect()

# df.to_sql('estro',con = conn , index = False , if_exists = 'replace')
# print("data inserted successfully")print(json.dumps(response["results"][0]["country"], indent=2))
cities = [{"city": "jaipur", "state": "rajasthan"}, {"city": "gurgaon", "state": "haryana"}, {"city": "mumbai", "state": "maharashtra"}]
for city in cities: 
    response = requests.get(f'https://api.geoapify.com/v1/geocode/search?text={city["city"]}%20{city["state"]}&format=json&apiKey=c5c5f4b9d23c4a8e8b816b777c11293a')
    response = response.json()

    for values in response["results"]:
        if values["state"] == city["state"] and values["city"] == city["city"]:
            print(values["lon"],values["lat"])
            break

    response = requests.get(f"http://www.7timer.info/bin/api.pl?lon={values["lon"]}&lat={values["lat"]}&product=astro&output=json")
    response = response.json()
    # print(json.dumps(response, indent=2))
    res = response["dataseries"]

    output = []
    for values in res:
        record = {}
        record["transparency"] = values["transparency"]
        record["wind_direction"] = values["wind10m"]["direction"]
        record["wind_speed"] = values["wind10m"]["speed"]
        record["temp2m"] = values["temp2m"]
        record["timepoint"] = values["timepoint"]
        output.append(record)
#     # print(output)

    df = pd.DataFrame(output) 
    hash_value=hash_pandas_object(df,index="False")
    df = df.assign(city = city["city"],df_hash = hash_value)
    print(df)
    print("DF printed successfully")

    server = "DESKTOP-QARI5IE\SQLEXPRESS"
    database = "projects"
    connection_string = (
        f"mssql+pyodbc://@{server}/{database}"
        "?driver=ODBC+Driver+18+for+SQL+Server"
        "&Trusted_Connection=yes"
        "&TrustServerCertificate=yes"
    )
    print(connection_string)
    engine = create_engine(connection_string)
    connection = engine.connect()

        # connection.execute(text("DELETE FROM projects.dbo.estro"))
        # connection.commit()

#         # result = connection.execute(text("SELECT * FROM dbo.astro"))
#         # for i in result:
#         #     print(i)
    # metadata = MetaData() 
  
    # estro = Table('estro', metadata, 
    #             Column('transparency', Integer), 
    #             Column('wind_direction', String), 
    #             Column('wind_speed', Integer),
    #             Column('temp2m', Integer),
    #             Column('timepoint', Integer),
    #             Column('city', String),
    #             Column('hs1', String)
    #             )

    # metadata = db.MetaData() #extracting the metadata
    # estro = db.Table('estro', metadata, autoload=True, autoload_with=engine) #Table object
    # print(repr(metadata.tables['estro']))

    
    for index, row in df.iterrows():
        # print(row['df_hash'])
        table = connection.execute(text(f"select case when count(*) > 0 then 1 else 0 end as is_hash_present from estro where hs1 = '{row['df_hash']}';"))
        result = table.first()[0]
        if result == 0:
            connection.execute(
        text("INSERT INTO estro ([transparency], [wind_direction], [wind_speed], [temp2m], [timepoint], [city], [hs1]) VALUES (:transparency, :wind_direction, :wind_speed, :temp2m, :timepoint, :city, :hs1)"),
        {
            "transparency": row['transparency'],
            "wind_direction": row['wind_direction'],
            "wind_speed": row['wind_speed'],
            "temp2m": row['temp2m'],
            "timepoint": row['timepoint'],
            "city": row['city'],
            "hs1": str(row['df_hash'])  # Ensure hs1 is treated as a string
        }
        )
            connection.commit()
        else:
         continue  
         
        
    

    #     print(result)

        # break
        
        # connection.execute(text("INSERT INTO estro([transparency],[wind_direction],[wind_speed],[temp2m],[timepoint],[city],[hs1]) values (?,?,?,?,?,?,?)"), 
        # {
        #     "transparency": row['transparency'],
        #     "wind_direction": row['wind_direction'],
        #     "wind_speed": row['wind_speed'],
        #     "temp2m": row['temp2m'],
        #     "timepoint": row['timepoint'],
        #     "city": row['city'],
        #     "hs1": str(row['df_hash'])  # Ensure hs1 is treated as a string
        # }) 
        

    connection.close()

               
#             # print(hs1)
#             # connection.execute(text()))
#         #     print(row)
#         #     break
    
#         # conn.commit()
#         # cursor.close()
#         # conn.close()


#         # df.to_sql('estro',con = conn , index = city , if_exists = 'append')
#         print("data inserted successfully")
#             # break
#     break

# # 1. for each row:
        # calculate hash of the row of df
            # - create a string by joining the values of each element of row
            # - pass that string to a hash function and store the hash value in a variable

        # if hash present in hash column in the table, then skip the row
            # create a sql query which checks whether the hash value exists in hash column
            # run the query using connection.execute and get output in 0 or 1
        # if hash not present in hash column in the table, then insert the row



