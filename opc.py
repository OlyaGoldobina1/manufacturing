from opcua import Client
from opcua.common.node import Node
from datetime import datetime
import os
import pandas as pd
import pg
from time import sleep
import json
from dotenv import load_dotenv
import notification

load_dotenv()

client = Client(os.getenv('opc_server'))
ape_data = os.getenv('api_data')
node_list = eval(os.getenv('api_data'))
login = json.loads(os.getenv('db_login'))
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
    return nodes_df

def start_opc(lst):
    while True:
        sleep(10)
        try:
            if(len(lst) > 0):
                break
            df = write_opc_data(node_list)
            if(df.size == 0):
                continue
            if df.status[df.status != 'Good'].any(axis=None):
                df[df.status != 'Good'].apply(lambda x: notification.send_message(f"""OPC {x.node_id} is dropped with status: {x.status}"""), axis = 1)
        except Exception as e:
            print(e)
            print('Mistake on cnc')