import json #json lilbrary
import time

import json_key_value
import cassandra_function     #cassandra_function.py
from  user_definition import *

#open a file ("input_file_name") as an input.
with open(input_file_name, 'r') as input_file:
    data = json.load(input_file)

#Connect to Cassandra
#https://datastax.github.io/python-driver/getting_started.html
#Instantiate a cluster
cluster = cassandra_function.Cluster()
session = cassandra_function.connect_session(cluster)

#Drop a keyspace
cassandra_function.drop_keyspace(session, keyspace)

# Create a keyspace
cassandra_function.create_keyspace(session, keyspace)

# Choose a keyspace
session.set_keyspace(keyspace)

# Create DB tables
table_name = 'items'
column_and_type_list = 'itemId INT, name VARCHAR, shortDesription TEXT,customerRating DOUBLE, numReviews INT, qty INT'
primary_key_list = 'itemId'
cassandra_function.create_table(session, table_name,column_and_type_list, primary_key_list)

json_data=open(input_file_name,'r')
data=json.load(json_data)
rows= data['items']
keys=['itemId', 'name', 'shortDesription','customerRating','numReviews']
for row_dic in rows:
    try:
        row_dic['customerRating']=float(row_dic['customerRating'])
        # convert customer rating from string to float if exists
    except:
        valid_keys = [key for key in keys if key in row_dic]
        val_row=[row_dic[key] for key in valid_keys]
        if 'itemId' in row_dic:
            qty=int(str(row_dic['itemId'])[-2:])
            valid_keys.append('qty')
            val_row.append(qty)
        val_row= tuple(val_row)
#        print val_row
        colnames= ','.join(valid_keys)
        fs='%s,'*len(valid_keys)
        fs='('+fs[0:-1]+')'
#        print "INSERT INTO %s (%s) VALUES %s"%(table_name,colnames,fs)
        session.execute("INSERT INTO %s (%s) VALUES %s"%(table_name,colnames,fs),val_row)

# Q3
# column_names = "*"
# table_name = 'items'
# constraint = 'name contains $$MLB Women''s San Francisco Giants Short Sleeve Top$$'
# cassandra_function.select_data(session,  table_name, column_names, constraint) # TRY IT AND COMMENT IT.

# Q4
question =  "Does select 'MLB Women''s San Francisco Giants Short Sleeve Top' work in the items table table work without ALLOW FILTERING?"
answer = "No"
print ("%s - %s") %(question, answer)
#
# # Q5
# Create Materized View
view_name = 'materialized_items_view'
column_names = '*'
table_name = 'items'
constraint = 'name IS NOT NULL'
primary_keys = 'name, itemId'
cassandra_function.create_materialized_view(session, view_name, column_names, table_name, constraint, primary_keys)

# Wait until materialized view is created.
select_table_query = "SELECT COUNT(*) AS ct FROM items"
base_table_row_count = session.execute(select_table_query)[0].ct
ct = 0
while ct != base_table_row_count:
    time.sleep(1)
    select_materialized_view_query = "SELECT COUNT(*) AS ct FROM materialized_items_view"
    rows = session.execute(select_materialized_view_query)
    ct= rows[0].ct

# Q6
# Select data from view
column_names = "*"
table_name = 'materialized_items_view'
constraint = '''name = $$MLB Women's San Francisco Giants Short Sleeve Top$$'''
returned_rows = cassandra_function.select_data(session,  table_name, column_names, constraint)
print returned_rows.current_rows   #PRINT ALL THE ITEMS.

# Q7
question = "Can you update data in materialized views?"
answer = "no"
print ("%s - %s") %(question, answer)

# Q8
question = "When you update data in a base(original) table, is the content also updated in the corresponding materialized view?"
answer = "yes"
print ("%s - %s") %(question, answer)

# Close communication.
cluster.shutdown()
