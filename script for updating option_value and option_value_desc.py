import mysql.connector
import pandas as pd
from datetime import datetime


conn = mysql.connector.connect(
    host="localhost", user="****", password="****", database="internship")

mycursor = conn.cursor()  # databse connection

file = 'AttributesSheet.xlsx'
df = pd.read_excel(file)

columns = list(df.columns)
# list(columns)
columns.pop(0)
columns.remove('MRP')
columns.remove('Price')
columns.remove('Brand')

inserting_values = {}
for column in columns:
    values = []
    temp = list(df[column])
    if column == 'Base Purchase Price':
        column = 'Price'
    for i in temp:
        if i not in values and pd.isnull(i) == False:
            values.append(i)
    inserting_values[column] = values


# existing value
sql_query = 'SELECT PRODUCT_OPTION_VAL_CODE FROM product_option_value;'
mycursor.execute(sql_query)
db_data = mycursor.fetchall()
existing_value = []
for i in db_data:
    existing_value.append(i[0])

keys = list(inserting_values.keys())
values = list(inserting_values.values())

# last POV_ID
sql_query = 'SELECT PRODUCT_OPTION_VALUE_ID FROM product_option_value;'
mycursor.execute(sql_query)
POV_ID = mycursor.fetchall()
LAST_POV_ID = POV_ID[len(POV_ID)-1][0]
# print(LAST_POV_ID)

# last DESC_ID
sql_query = 'SELECT DESCRIPTION_ID FROM product_option_value_description;'
mycursor.execute(sql_query)
DESC_ID = mycursor.fetchall()
LAST_DESC_ID = DESC_ID[len(DESC_ID)-1][0]
# print(LAST_DESC_ID)

# making options based on existing DB
final_option = []
final_desc = []
for i in range(len(keys)):
    temp = keys[i].split()
    temp1 = "_".join(temp)
    checking = f"{temp1.upper()}_VALUE_{1:02d}"

    if checking in existing_value:
        inserting_values[keys[i]].pop(0)
        for j in range(0, len(inserting_values[keys[i]])):
            POVC = f"{temp1.upper()}_VALUE_{j+2:02d}"
            final_option.append(POVC)
        for p in list(inserting_values[keys[i]]):
            final_desc.append(p)
    else:
        if len(inserting_values[keys[i]]) == 0:
            pass
        else:
            for j in range(len(inserting_values[keys[i]])):
                POVC = f"{temp1.upper()}_VALUE_{j+1:02d}"
                final_option.append(POVC)
            for p in list(inserting_values[keys[i]]):
                final_desc.append(p)

# current date


# updating DB
for i in range(len(final_option)):
    sql_query = '''INSERT INTO product_option_value (PRODUCT_OPTION_VALUE_ID, PRODUCT_OPTION_VAL_CODE, PRODUCT_OPT_FOR_DISP, PRODUCT_OPT_VAL_IMAGE, PRODUCT_OPT_VAL_SORT_ORD, MERCHANT_ID)
                    VALUES (%s, %s, %s, %s, %s, %s)'''
    records = (LAST_POV_ID+2+i, final_option[i], 0, None, None, 1)
    mycursor.execute(sql_query, records)
    conn.commit()

    sql_query = '''INSERT INTO product_option_value_description (DESCRIPTION_ID, DATE_CREATED, DATE_MODIFIED, UPDT_ID, DESCRIPTION, NAME, TITLE, LANGUAGE_ID, PRODUCT_OPTION_VALUE_ID)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)'''
    date_created = str(datetime.now().replace(microsecond=0))
    date_modified = str(datetime.now().replace(microsecond=0))
    records = (LAST_DESC_ID+1+i, date_created, date_modified,
               None, None, final_desc[i], None, 1, LAST_POV_ID+2+i)
    mycursor.execute(sql_query, records)
    conn.commit()

if conn.is_connected():
    mycursor.close()
    conn.close()
