from cassandra.cluster import Cluster

def connect_session(cluster):
    #Establish a connection
    cluster = Cluster(['127.0.0.1'])
    return  cluster.connect()

def execute(session, query):
    return session.execute(query)

def create_keyspace(session, keyspace):
    create_keyspace_query =  '''CREATE KEYSPACE %s WITH replication=
    {'class':'SimpleStrategy','replication_factor':'1'}'''%keyspace
    execute(session, create_keyspace_query)

def drop_keyspace(session, keyspace):
    drop_keyspace_query = "DROP KEYSPACE %s" %keyspace
    execute(session, drop_keyspace_query)

def create_table(session, table_name, column_and_type_list, primary_key_list):
    create_table_query =  "CREATE TABLE %s(%s, PRIMARY KEY (%s));"%(table_name,column_and_type_list,primary_key_list)
    execute(session, create_table_query)


# def insert_into_table(session, table_name, column_names, values):
#     execute(session, "INSERT INTO %s (%s) VALUES (%s)"%(table_name,column_names,fs)
#                              +',%s'%values

def select_data(session, table_name, column_names, constraint):
    select_data_query = '''SELECT %s FROM %s WHERE %s'''%(column_names,table_name,constraint)
#    print select_data_query
    return execute(session, select_data_query)

def update_data(session, table_name, column, value, constraint):
    update_data_query = "UPDATE %s SET %s = %s WHERE %s" %(table_name, column, value, constraint)
    execute(session, update_data_query)

def create_materialized_view(session, view_name, column_names, table_name, constraints, primary_keys):
    create_materialized_view_query = '''CREATE MATERIALIZED VIEW %s AS select %s
    from %s where %s PRIMARY KEY (%s);'''%(view_name,column_names,table_name,constraints,primary_keys)
    execute(session, create_materialized_view_query)



