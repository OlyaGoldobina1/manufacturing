import paho.mqtt.client as mqtt
import datetime
import pg
import pandas as pd
import os
import json
from dotenv import load_dotenv
import notification

load_dotenv()
dict_value = {}

def on_message(client, userdata, msg, list):
    global dict_value
    if(len(list) > 0):
        client.disconnect()
        return
    mqtt_source, mqtt_value = msg.topic.split("/")
    time = datetime.datetime.now()
    if mqtt_source == "sens":
        if mqtt_value[0] == 't':
            dict_value['real'] = {"hem": None, "temp": [msg.payload.decode()], "timestamp": None,\
                         "source": "real"}
        elif mqtt_value[0] == 'h' and dict_value.get('real') is not None:
            dict_value['real']["hem"] = [msg.payload.decode()]
    else:
        if mqtt_source == "sensor":
            if mqtt_value[0] == 't':
                dict_value['demo'] = {"hem": None, "temp": [msg.payload.decode()], "timestamp": None,\
                            "source": "demo"}
            elif mqtt_value[0] == 'h' and dict_value.get('demo') is not None:
                dict_value['demo']["hem"] = [msg.payload.decode()]
    if dict_value.get('real') is not None:
        if dict_value['real']['hem'] is not None and dict_value['real']['temp'] is not None:
            dict_value['real']['timestamp'] = [time]
            df = pd.DataFrame(dict_value['real'])
            pg.insert_table(df, 'mqtt')
            if int(dict_value['real']['hem'][0]) > 70:
                notification.send_message(f"""MQTT Машина {dict_value['real']['source'][0]} имеет влажность {dict_value['real']['hem'][0]}""")
            if int(dict_value['real']['temp'][0]) > 30:
                notification.send_message(f"""MQTT Машина {dict_value['real']['source'][0]} имеет температуру {dict_value['real']['temp'][0]}""")
            dict_value['real'] = None
    if dict_value.get('demo') is not None:
        if dict_value['demo']['hem'] is not None and dict_value['demo']['temp'] is not None:
            dict_value['demo']['timestamp'] = [time]
            df = pd.DataFrame(dict_value['demo']) 
            pg.insert_table(df, 'mqtt')
            # if int(dict_value['demo']['hem'][0]) > 70:
            #     notification.send_message(f"""MQTT Машина {dict_value['demo']['source'][0]} имеет влажность {dict_value['demo']['hem'][0]}""")
            # if int(dict_value['demo']['temp'][0]) > 30:
            #     notification.send_message(f"""MQTT Машина {dict_value['demo']['source'][0]} имеет температуру {dict_value['demo']['temp'][0]}""")
            dict_value['demo'] = None



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
    while True:
        try:
            if(len(list) > 0):
                break
            mqtt_client = init_mqtt()
            mqtt_loop(mqtt_client, list)
        except Exception as e:
            print(e)
            print('Mistake on mqtt')