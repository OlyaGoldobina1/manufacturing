from opcua import Client
from opcua.common.node import Node
from datetime import datetime
import os
import pandas as pd


client = Client(os.environ['opc_server'])
nodes = []

def get_opc_data(node_list: List[srt]) -> DataFrame:
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
    return DataFrame(nodes_data)

def write_opc_data(node_list: List[srt]):
    nodes_df = get_opc_data(node_list)
    db.insert_table(nodes_df, 'cnc', login=os.environ['db_login'])