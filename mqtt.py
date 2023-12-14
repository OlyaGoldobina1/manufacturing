import paho.mqtt.client as mqtt
import datetime
import pandas


def on_message(client, userdata, msg):
    mqtt_data = {"topic": msg.topic[-1], "value": msg.payload.decode(), "timestamp": datetime.now()}
    df = pd.DataFrame(mqtt_data)
    db.insert_table(df, 'mqtt', login=os.environ['db_login'])
    print(float(msg.payload.decode()), msg.topic[-1])


def init_mqtt():
    client = mqtt.Client()
    client.username_pw_set("admin1","@dm!N")
    client.connect('82.146.60.95', 1883, 4)
    return client

def mqtt_loop(client):
    client.subscribe("sens/t", qos=0)
    client.subscribe("sens/h", qos=0)
    client.on_message = on_message
    client.loop_forever()


mqtt_client = init_mqtt()
mqtt_loop(mqtt_client)