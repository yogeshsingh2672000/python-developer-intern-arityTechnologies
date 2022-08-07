import mysql.connector
import pandas as pd
from datetime import date, datetime


conn = mysql.connector.connect(
    host="localhost", user="****", password="****", database="internship")

mycursor = conn.cursor()  # databse connection

file = 'AttributesSheet.xlsx'
df = pd.read_excel(file)


dimensions = []
for i in list(df["Dimensions"]):
    temp = i.split("*")
    dimensions.append(temp)

pids = list(df["PID"])

# last PRODUCT_ID
sql_query = "SELECT PRODUCT_ID FROM product"
mycursor.execute(sql_query)
PRODUCT_IDs = mycursor.fetchall()
last_product_id = PRODUCT_IDs[-1][0]


sql_query = "SELECT SKU FROM product"
mycursor.execute(sql_query)
db_data = mycursor.fetchall()
existing_data = []
for i in db_data:
    existing_data.append(i[0])

# removing exiting data from pids and dimensions
for i in existing_data:
    if i in pids:
        index = pids.index(i)
        pids.pop(index)
        dimensions.pop(index)

# Removing duplicates from pids
new_pids = []
for i in range(len(pids)):
    if pids[i] not in new_pids:
        new_pids.append(pids[i])
    else:
        dimensions.pop(i)

# updating DB
for i in range(len(dimensions)):
    date_created = str(datetime.now().replace(microsecond=0))
    date_modified = str(datetime.now().replace(microsecond=0))
    width = dimensions[i][0]
    height = dimensions[i][1]
    length = dimensions[i][2]
    product_id = f"{last_product_id+i+1:02d}"
    sql_query = '''INSERT INTO product
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'''
    records = (product_id, date_created, date_modified, None, 1, None, date_modified, 0, height, 0,
               length, None, None, None, 0, 0, 1, width, new_pids[i], None, None, None, new_pids[i], 0, 1, 1, None, 1, 1)
    mycursor.execute(sql_query, records)
    conn.commit()

if conn.is_connected():
    mycursor.close()
    conn.close()
