import mysql.connector
import pandas as pd

conn = mysql.connector.connect(
    host="localhost", user="****", password="****", database="internship")

mycursor = conn.cursor()  # databse connection

file = 'AttributesSheet.xlsx'
df = pd.read_excel(file)


# fetching data from product_option_desc
sql_query = "SELECT NAME, PRODUCT_OPTION_ID FROM product_option_desc"
mycursor.execute(sql_query)
db_data = mycursor.fetchall()

product_opt_desc = {}
for i in db_data:
    product_opt_desc[i[0]] = i[1]


# fetching data from product_option_value_description
sql_query = "SELECT NAME, PRODUCT_OPTION_VALUE_ID FROM product_option_value_description"
mycursor.execute(sql_query)
db_data = mycursor.fetchall()

product_opt_val_desc = {}
for i in db_data:
    product_opt_val_desc[i[0]] = i[1]

# fetching data from product table
sql_query = "SELECT PRODUCT_ID FROM product"
mycursor.execute(sql_query)
db_data = mycursor.fetchall()

product_id = []
for i in db_data:
    product_id.append(i[0])

# removing false data
product_id.pop(0)

sql_query = "SELECT PRODUCT_ID FROM product_attribute"
mycursor.execute(sql_query)
db_data = mycursor.fetchall()

existing_product_id = []
for i in db_data:
    existing_product_id.append(i[0])

# removing existing value from existing_product_id
for i in existing_product_id:
    if i in product_id:
        product_id.pop(product_id.index(i))


null_value_keys = ['COLOR CODE', 'LENS WIDTH', 'GENDER']
non_null_values = ['DIMENSIONS', 'BRAND', 'ORIGINAL BRAND', 'BUY_PRICE', 'COLOR',
                   'MATERIAL', 'SHAPE', 'LENS HEIGHT', 'SIMILAR PIDS', 'MRP', 'PRICE', 'CLEARANCE']

dimensions = list(df[non_null_values[0].title()])
dimensions.pop(0)

brand = list(df[non_null_values[1].title()])
brand.pop(0)

original_price = list(df[non_null_values[2].title()])
original_price.pop(0)

base_purchase_price = list(df["Base Purchase Price"])
base_purchase_price.pop(0)

color = list(df[non_null_values[4].title()])
color.pop(0)

material = list(df[non_null_values[5].title()])
material.pop(0)

shape = list(df[non_null_values[6].title()])
shape.pop(0)

lens_height = list(df[non_null_values[7].title()])
lens_height.pop(0)

similar_pids = list(df["Similar PIDs"])
similar_pids.pop(0)

mrp = list(df["MRP"])
mrp.pop(0)

price = list(df[non_null_values[10].title()])
price.pop(0)

clearance = list(df[non_null_values[11].title()])
clearance.pop(0)


columns = [dimensions, brand, original_price, base_purchase_price, color,
           material, shape, lens_height, similar_pids, mrp, price, clearance]

non_null_values = ['DIMENSIONS', 'BRAND', 'ORIGINAL BRAND', 'BUY_PRICE', 'COLOR',
                   'MATERIAL', 'SHAPE', 'LENS HEIGHT', 'SIMILAR PIDS', 'MRP', 'PRICE', 'CLEARANCE']


insertion_data = []
# print(product_opt_val_desc)
for id in range(len(product_id)):
    for key in non_null_values+null_value_keys:
        if key in null_value_keys:
            pass
        elif key == "DIMENSIONS":
            temp = columns[0]
            index = product_id.index(product_id[id])
            insertion_data.append(
                [product_id[id], product_opt_desc[key], product_opt_val_desc[temp[index]]])
        elif key == "BRAND":
            temp = columns[1]
            index = product_id.index(product_id[id])
            insertion_data.append(
                [product_id[id], product_opt_desc[key], product_opt_val_desc[temp[index].upper()]])
        elif key == "ORIGINAL BRAND":
            temp = columns[2]
            index = product_id.index(product_id[id])
            if temp[index] == "Bolo":
                insertion_data.append(
                    [product_id[id], product_opt_desc[key], product_opt_val_desc[temp[index].upper()]])
            else:
                insertion_data.append(
                    [product_id[id], product_opt_desc[key], product_opt_val_desc[temp[index]]])
        elif key == "Buy_Price".upper():
            temp = columns[3]
            index = product_id.index(product_id[id])
            insertion_data.append(
                [product_id[id], product_opt_desc[key], product_opt_val_desc[str(temp[index])]])
        elif key == "COLOR":
            temp = columns[4]
            index = product_id.index(product_id[id])
            if temp[index] == 'Pine Tree (Black+Green) #122e05':
                insertion_data.append(
                    [product_id[id], product_opt_desc[key], 103])
            else:
                insertion_data.append(
                    [product_id[id], product_opt_desc[key], product_opt_val_desc[temp[index]]])
        elif key == "MATERIAL":
            temp = columns[5]
            index = product_id.index(product_id[id])
            insertion_data.append(
                [product_id[id], product_opt_desc[key], product_opt_val_desc[temp[index]]])
        elif key == "SHAPE":
            temp = columns[6]
            index = product_id.index(product_id[id])
            insertion_data.append(
                [product_id[id], product_opt_desc[key], product_opt_val_desc[str(temp[index])]])
        elif key == "LENS HEIGHT":
            temp = columns[7]
            index = product_id.index(product_id[id])
            insertion_data.append(
                [product_id[id], product_opt_desc[key], product_opt_val_desc[str(temp[index])]])
        elif key == "SIMILAR PIDS":
            temp = columns[8]
            index = product_id.index(product_id[id])
            value = ["PID0112", "PID0115", "PID0120, PID0119",
                     "PID0114, PID0113", "PID0122,PID0114, PID0113", "PID0137"]
            if str(temp[index]) not in value:
                pass
            else:
                insertion_data.append(
                    [product_id[id], product_opt_desc[key], product_opt_val_desc[temp[index]]])
        elif key == "MRP":
            pass
        elif key == "PRICE":
            pass
        elif key == "CLEARANCE":
            temp = columns[11]
            index = product_id.index(product_id[id])
            value = 'Yes'
            if temp[index] != value:
                pass
            else:
                insertion_data.append(
                    [product_id[id], product_opt_desc[key], product_opt_val_desc[temp[index]]])

# last product_attribute_id
sql_query = "SELECT PRODUCT_ATTRIBUTE_ID from product_attribute"
mycursor.execute(sql_query)
db_data = mycursor.fetchall()
last_product_attribute_id = db_data[-1][0]

for i in range(len(insertion_data)):
    sql_query = """INSERT INTO internship.product_attribute 
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
    record = (last_product_attribute_id+1+i, 0, 0, 1, 0, 0, 0.00, 0.00,
              0, insertion_data[i][0], insertion_data[i][1], insertion_data[i][2])
    mycursor.execute(sql_query, record)
    conn.commit()

if conn.is_connected():
    mycursor.close()
    conn.close()
