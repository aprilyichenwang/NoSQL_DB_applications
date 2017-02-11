
def database(client, dbname):
    return client[dbname]

def import_query(dbname, collection_name, input_file_name):
    # import the entire document from walmart_search_san_francisco.json
    # to given collection
    query=''' mongoimport --db %s --collection %s --file %s'''%(dbname,collection_name,input_file_name)
    return query


def drop_table_query(db, collection_name):
    db[collection_name].drop()

