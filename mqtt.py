import paho.mqtt.client as mqtt
import datetime
import pg
import pandas as pd
import os
import json
from dotenv import load_dotenv

load_dotenv()

login = json.loads(os.getenv('db_login'))

def on_message(client, userdata, msg, list):
    if(len(list) > 0):
        client.disconnect()
        return
    mqtt_source, mqtt_value = msg.topic.split("/")
    time = datetime.datetime.now()
    if mqtt_source == "sens":
        mqtt_data = {"topic": [mqtt_value[0]], "value": [msg.payload.decode()], "timestamp": [time],\
                     "source": "real"}
    else:
        if mqtt_source == "sensor":
            mqtt_data = {"topic": [mqtt_value[0]], "value": [msg.payload.decode()], "timestamp": [time],\
                     "source": "demo"}
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
    client.subscribe("sensor/temp", qos=0)
    client.subscribe("sensor/hump", qos=0)
    client.on_message = lambda client, userdata, msg : on_message(client, userdata, msg, list)
    client.loop_forever()


def start_mqtt(list):
    mqtt_client = init_mqtt()
    mqtt_loop(mqtt_client, list)