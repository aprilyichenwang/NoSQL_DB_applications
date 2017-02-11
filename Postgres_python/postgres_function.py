import psycopg2

def connectdb(db_name, user_name):
    try:
        db_conn = psycopg2.connect(dbname=db_name, user=user_name)
    except:
        print("Not able to connect to " + db_name)
    return db_conn

def db_cursor(db_conn):
    cursor = db_conn.cursor()  # open a cursor to perform db operations.
    return cursor

def execute(db_cursor, query):
    db_cursor.execute(query)

# table_name = 'inventory'
# column_and_type_list = 'itemId INTEGER,qty INTEGER'
def create_table(db_cursor, table_name, column_and_type_list):
    create_table_query = 'CREATE TABLE %s (%s);' % (table_name, column_and_type_list)
    execute(db_cursor,create_table_query)


def drop_table(db_cursor, table_name):
    drop_table_query = "DROP TABLE " + table_name + ";"
    execute(db_cursor, drop_table_query)

def select_data(db_cursor, table_name, column_name, constraint):
    select_data_query =  "SELECT %s FROM %s WHERE %s"%(column_name,table_name,constraint)
    execute(db_cursor, select_data_query)

def insert_into_table(db_cursor, table_name, column_names, values):
    colnames = column_names.split(',')
    Q2 = '%s,' * len(colnames)
    Q2 = '('+Q2[0:-1]+');'
    Query='''INSERT INTO %s (%s) VALUES %s'''%(table_name,column_names,Q2)
    db_cursor.execute(Query, values)


