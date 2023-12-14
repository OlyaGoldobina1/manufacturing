from opcua import Client
from opcua.common.node import Node
from datetime import datetime
import os
import pandas as pd
import pg
from time import sleep
import json


client = Client(os.environ['opc.tcp://opcua.demo-this.com:51210/UA/SampleServer'])
ape_data ='["ns=2;i=10869", "ns=2;i=10853", "ns=2;i=10849"]'
node_list = '["ns=2;i=10869", "ns=2;i=10853", "ns=2;i=10849"]'
login = '{"dbname": "postgres","user": "postgres","password": "posrgres","host": "212.233.99.38","port": "5432","client_encoding": "utf-8"}'
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

def start_opc(lst):
    while True:
        sleep(10)
        try:
            if(len(lst) > 0):
                break
            write_opc_data(node_list)
        except Exception as e:
            print(e)
            print('Mistake on opc')