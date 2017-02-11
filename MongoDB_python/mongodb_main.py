
from pymongo import MongoClient # Do pip install for using this lib.
import subprocess
import mongodb_query
from  user_definition import *

#Create connection 
client = MongoClient() #default-localhost:27017
#Connect to database
db = mongodb_query.database(client, dbname)


# Drop table.
mongodb_query.drop_table_query(db, collection_name)


# import the entire document from walmart_search_san_francisco.json to given collection
mongoimport_query = mongodb_query.import_query(dbname, collection_name, input_file_name)
subprocess.call(mongoimport_query,shell=True)


# Add the field qty  being the last two digits of itemId and add it into the document.
def insert_field():
    json_data=db.items.find_one()
    for i in range(len(json_data['items'])):
        json_data['items'][i]['qty']=int(str(json_data['items'][i]['itemId'])[-2:])

    items=db.items
    items.update({"facets":[]},json_data)
insert_field()


# Find First MLB Women's San Francisco Giants Short Sleeve Tops.
cursor=db.items.find(
    {"items.name":"MLB Women's San Francisco Giants Short Sleeve Top"},
    {"items.name.$":1})
print cursor.next()
# return the next document in the cursor that is returned by db.collection.find()



