import json

def count_data(data, key):
    return (len(data[key]))

def get_data_value(data,key):
    if(data.get(key)):
        return data[key]
    else:
        return 'NULL'
