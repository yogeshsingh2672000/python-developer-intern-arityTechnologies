import mysql.connector
import pandas as pd
from datetime import datetime

conn = mysql.connector.connect(
    host="localhost", user="****", password="****", database="internship")

mycursor = conn.cursor()  # databse connection


def get_column():
    file = 'AttributesSheet.xlsx'
    newData = pd.read_excel(file)
    return newData.columns


columns = list(get_column())
columns.pop(0)

for column in columns:
    output = []

    sql_query = 'SELECT Name FROM product_option_desc;'
    mycursor.execute(sql_query)
    db_data = mycursor.fetchall()
    db_columns = []
    for i in db_data:
        db_columns.append(i[0])

    for value in columns:
        if value.upper() not in db_columns:
            output.append(value)

output.remove('Base Purchase Price')

insertion_data = {}
for i in range(0, len(output)):
    insertion_data['PRODUCT_OPTION_'+str(i+len(db_data)+1)] = output[i]

# Last Option Id
sql_query = 'SELECT PRODUCT_OPTION_ID FROM product_option;'
mycursor.execute(sql_query)
option_ids = mycursor.fetchall()
last_option_id = option_ids[len(option_ids)-1][0]


# Last description Id
sql_query = 'SELECT DESCRIPTION_ID FROM product_option_desc;'
mycursor.execute(sql_query)
desc_ids = mycursor.fetchall()
last_desc_id = desc_ids[len(desc_ids)-1][0]
# print(last_desc_id)

# Inserting data in product_option table
product_ids = list(insertion_data.keys())
for i in range(0, len(product_ids)):
    po_ID = last_option_id+i+1
    sql_query = '''INSERT INTO product_option (PRODUCT_OPTION_ID, PRODUCT_OPTION_CODE, PRODUCT_OPTION_SORT_ORD, PRODUCT_OPTION_TYPE, PRODUCT_OPTION_READ, MERCHANT_ID) 
                    VALUES (%s, %s, %s, %s, %s, %s)'''
    records = (po_ID, product_ids[i], None, 'text', 0, 1)
    mycursor.execute(sql_query, records)
    conn.commit()

# Inserting data in product_option_desc
product_values = list(insertion_data.values())
for i in range(0, len(product_values)):
    po_ID = last_option_id+i+1
    desc_id = last_desc_id+i+1
    sql_query = """INSERT INTO product_option_desc (DESCRIPTION_ID, DATE_CREATED, DATE_MODIFIED, UPDT_ID, DESCRIPTION, NAME, TITLE, PRODUCT_OPTION_COMMENT, LANGUAGE_ID, PRODUCT_OPTION_ID)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
    # current_time = str(datetime.now())
    date_created = str(datetime.now().replace(microsecond=0))
    date_modified = str(datetime.now().replace(microsecond=0))
    records = (desc_id, date_created, date_modified, None,
               None, product_values[i].upper(), None, None, 1, po_ID)
    mycursor.execute(sql_query, records)
    conn.commit()

if conn.is_connected():
    mycursor.close()
    conn.close()
