import mysql.connector
import pandas as pd
from datetime import date, datetime


conn = mysql.connector.connect(
    host="localhost", user="****", password="****", database="internship")

mycursor = conn.cursor()  # databse connection

file = 'AttributesSheet.xlsx'
df = pd.read_excel(file)

# product ID
product_ids = []
sql_query = "SELECT PRODUCT_ID FROM product"
mycursor.execute(sql_query)
db_data = mycursor.fetchall()

for i in db_data:
    product_ids.append(i[0])

# removing trial data comming from db
product_ids.pop(0)

existing_ids = []
sql_query = "SELECT PRODUCT_ID FROM product_availability"
mycursor.execute(sql_query)
db_data = mycursor.fetchall()

for i in db_data:
    existing_ids.append(i[0])

# removing existing data from product_ids
for i in existing_ids:
    if i in product_ids:
        product_ids.remove(i)

# last product_avail_id
last_product_avail_id = db_data[-1][0]
sql_query = "SELECT PRODUCT_AVAIL_ID FROM product_availability"
mycursor.execute(sql_query)
db_data = mycursor.fetchall()

# updating product_availability
for i in range(len(product_ids)):
    sql_query = """INSERT INTO product_availability VALUES
                    (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
    records = (last_product_avail_id+1+i, None, None, None, None,
               None, None, 0, 1, 1, 1, 1, "*", None, None, product_ids[i])
    mycursor.execute(sql_query, records)
    conn.commit()

if conn.is_connected():
    mycursor.close()
    conn.close()
