import paho.mqtt.client as mqtt
import datetime
import pg
import pandas as pd
import os
import json
import re
from dotenv import load_dotenv
import notification

load_dotenv()
dict_value = {}
error_dict = {}
source = {"sens": "real", "sensor": "demo"}

def on_message(client, userdata, msg, list):
    global dict_value
    global error_dict
    global source
    if(len(list) > 0):
        client.disconnect()
        return
    mqtt_source, mqtt_value = msg.topic.split("/")
    time = datetime.datetime.now()
    # if mqtt_source == "sens":
    # print(msg.payload.decode())
    if mqtt_value[0] == 't' and re.match(r"^[-+]?[0-9]*\.?[0-9]+(e[-+]?[0-9]+)?$", msg.payload.decode()) is not None:
        dict_value[source[mqtt_source]] = {"hem": None, "temp": [msg.payload.decode()], "timestamp": None,\
                        "source": source[mqtt_source], "error": False}
        error_dict[mqtt_source+mqtt_value[0]] = False
    elif mqtt_value[0] == 'h' and dict_value.get(source[mqtt_source]) is not None and re.match(r"^[-+]?[0-9]*\.?[0-9]+(e[-+]?[0-9]+)?$", msg.payload.decode()) is not None:
        dict_value[source[mqtt_source]]["hem"] = [msg.payload.decode()]
        error_dict[mqtt_source+mqtt_value[0]] = False
    elif re.match(r"^[-+]?[0-9]*\.?[0-9]+(e[-+]?[0-9]+)?$", msg.payload.decode()) is None:
        df = pd.DataFrame({"hem": None, "temp": None, "timestamp": [time],\
                        "source": source[mqtt_source], "error": True})
        pg.insert_table(df, 'mqtt')
        if (error_dict.get(mqtt_source+mqtt_value[0]) is None or error_dict.get(mqtt_source+mqtt_value[0]) == False):
            error_dict[mqtt_source+mqtt_value[0]] = True
            if source[mqtt_source] == 'real':
                notification.send_message(f"""MQTT Машина {source[mqtt_source]} имеет отключенный сенсор {mqtt_value}""")
    if dict_value.get(source[mqtt_source]) is not None:
        if dict_value[source[mqtt_source]]['hem'] is not None and dict_value[source[mqtt_source]]['temp'] is not None:
            dict_value[source[mqtt_source]]['timestamp'] = [time]
            df = pd.DataFrame(dict_value[source[mqtt_source]])
            pg.insert_table(df, 'mqtt')
            if source[mqtt_source] == 'real':
              if float(dict_value[source[mqtt_source]]['hem'][0]) > 70:
                 notification.send_message(f"""MQTT Машина {dict_value[source[mqtt_source]]['source'][0]} имеет влажность {dict_value[source[mqtt_source]]['hem'][0]}""")
              if float(dict_value[source[mqtt_source]]['temp'][0]) > 30:
                   notification.send_message(f"""MQTT Машина {dict_value[source[mqtt_source]]['source'][0]} имеет температуру {dict_value[source[mqtt_source]]['temp'][0]}""")
            dict_value[source[mqtt_source]] = None



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