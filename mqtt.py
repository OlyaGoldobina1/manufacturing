import paho.mqtt.client as mqtt
import datetime
import pandas
import pg
import pandas as pd
import os
import json

login = '{"dbname": "postgres","user": "postgres","password": "posrgres","host": "212.233.99.38","port": "5432","client_encoding": "utf-8"}'

def on_message(client, userdata, msg, list):
    if(len(list) > 0):
        client.disconnect()
        return
    mqtt_data = {"topic": [msg.topic[-1]], "value": [msg.payload.decode()], "timestamp": [datetime.datetime.now()]}
    df = pd.DataFrame(mqtt_data)
    pg.insert_table(df, 'mqtt', login=login)


def init_mqtt():
    client = mqtt.Client()
    client.username_pw_set("admin1","@dm!N")
    client.connect('82.146.60.95', 1883, 4)
    return client

def mqtt_loop(client, list):
    client.subscribe("sens/t", qos=0)
    client.subscribe("sens/h", qos=0)
    client.on_message = lambda client, userdata, msg : on_message(client, userdata, msg, list)
    client.loop_forever()


def start_mqtt(list):
    mqtt_client = init_mqtt()
    mqtt_loop(mqtt_client, list)