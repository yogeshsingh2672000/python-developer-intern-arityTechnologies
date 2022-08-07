import mysql.connector
import pandas as pd
from datetime import date, datetime


conn = mysql.connector.connect(
    host="localhost", user="****", password="****", database="internship")

mycursor = conn.cursor()  # databse connection

file = 'AttributesSheet.xlsx'
df = pd.read_excel(file)

mrp = []
for i in df["MRP"]:
    mrp.append(i)

# removing 0th index due to db updation
mrp.pop(0)


price = []
for i in df["Price"]:
    price.append(i)

# removing 0th index due to db updation
price.pop(0)

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
sql_query = "SELECT PRODUCT_AVAIL_ID FROM product_price"
mycursor.execute(sql_query)
db_data = mycursor.fetchall()

for i in db_data:
    existing_ids.append(i[0])

# removing existing data from product_ids
for i in existing_ids:
    if i in product_ids:
        product_ids.remove(i)


# last product_price_id
last_product_price_ids = db_data[-1][0]
sql_query = "SELECT PRODUCT_PRICE_ID FROM product_price"
mycursor.execute(sql_query)
db_data = mycursor.fetchall()


for i in range(len(product_ids)):
    sql_query = """INSERT INTO product_price VALUES
                    (%s, %s, %s, %s, %s, %s, %s, %s, %s)"""
    records = (last_product_price_ids+1+i, "base", 1,
               mrp[i], price[i], None, None, "ONE_TIME", product_ids[i])
    mycursor.execute(sql_query, records)
    conn.commit()

# last_description_id
sql_query = "SELECT DESCRIPTION_ID FROM product_price_description"
mycursor.execute(sql_query)
db_data = mycursor.fetchall()
last_description_id = db_data[-1][0]


for i in range(len(product_ids)):
    sql_query = """INSERT INTO product_price_description VALUES
                    (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
    date_created = str(datetime.now().replace(microsecond=0))
    date_modified = str(datetime.now().replace(microsecond=0))
    records = (last_description_id+1+i, date_created, date_modified,
               None, None, "DEFAULT", None, None, 1, product_ids[i])
    mycursor.execute(sql_query, records)
    conn.commit()

if conn.is_connected():
    mycursor.close()
    conn.close()
