import json
import json_key_value
import postgres_function
from  user_definition import *

with open(input_file_name, 'r') as input_file:
    data = json.load(input_file)
    print(json_key_value.get_data_value(data, "totalResults"))
    print(json_key_value.count_data(data, "items"))

# open database
db_conn = postgres_function.connectdb(dbname, usr_name)
cursor = postgres_function.db_cursor(db_conn)

#Create DB tables, inventory and items
#TODO : WRITE CODE TO INSERT DATA FROM "data" in line 9. USE insert_into_table() FUNCTION IN postgres_function.py
# Ex.  postgres_function.insert_into_table(cursor, "items", "itemId, name", "VALUE, VALUE")

def create_table_dict(colnames):
    # create a tuple of dictionaries, with each entry being a dictionary with the keys being the colnames
    rows = []
    for i in range(json_key_value.count_data(data, "items")):
        item = {}
        for colname in colnames:
            colname=colname.strip()
            item[colname]=data['items'][i].get(colname,None)
        rows.append(item)
    rows=tuple(rows)
    return rows

def get_colnames_string(column_and_type_list_items):
    ls=column_and_type_list_items.split(',')
    col_stringls=[l.strip().split(' ')[0] for l in ls]
    col_string= ','.join(col_stringls)
    return col_string

def insert_content_TB(table_name,column_and_type_list_items):
    colnames_string = get_colnames_string(column_and_type_list_items)
    colnames = colnames_string.split(',')
    rows=create_table_dict(colnames)
    for row in rows:
        if table_name=='inventory':
            id = row['itemId']
            qty = int(str(id)[-2:])
            row['qty'] = qty
        values=tuple(row[i]for i in colnames)
        postgres_function.insert_into_table(cursor, table_name, colnames_string, values)


# CREATE AND INSERT TABLE CONTENT - inventory
column_and_type_list_inventory='itemId INTEGER,qty INTEGER, name CHAR(1000)'
postgres_function.create_table(cursor,'inventory',column_and_type_list_inventory)
insert_content_TB('inventory',column_and_type_list_inventory)


# CREATE AND INSERT TABLE CONTENT - itemtable
column_and_type_list_items = '''itemId INTEGER,name VARCHAR,shortDescription TEXT,
                        customerRating REAL, numReviews INTEGER'''
postgres_function.create_table(cursor,'itemtable',column_and_type_list_items)
insert_content_TB('itemtable',column_and_type_list_items)

# Q4 Select Data
table_name = 'inventory'
column_names = "itemId,qty"
constraint = '''name= $$MLB Women's San Francisco Giants Short Sleeve Top$$ '''

postgres_function.select_data(cursor, table_name, column_names, constraint)
print cursor.fetchone()
db_conn.commit()

# close communication with database.
cursor.close()
db_conn.close()

