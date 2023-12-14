from opcua import Client
from opcua.common.node import Node
from datetime import datetime
import os
import pandas as pd
import pg
from time import sleep
import json


client = Client(os.environ['opc_server'])
ape_data = os.environ['api_data']
node_list = eval(os.environ['api_data'])
login = json.loads(os.environ['db_login'])
prev = ''

def get_opc_data(node_list):
    client.connect()
    nodes_data = []
    for node in node_list:
        node_data = client.get_node(node)
        value = node_data.get_data_value().Value.Value
        status = node_data.get_data_value().StatusCode.name
        time = node_data.get_data_value().SourceTimestamp
        nodes_data.append({
            'node_id': node,
            'value': value,
            'status': status,
            'timestamp': time
        })
    client.disconnect()
    return pd.DataFrame(nodes_data)

def write_opc_data(node_list):
    nodes_df = get_opc_data(node_list)
    pg.insert_table(nodes_df, 'opc', login=login)

def start_opc():
    while True:
        sleep(10)
        try:
            write_opc_data(node_list)
        except Exception as e:
            print(e)
            print('Mistake on opc')