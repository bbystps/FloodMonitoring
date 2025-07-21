import paho.mqtt.client as mqtt
import time
import sys

broker = "3.27.210.100"
port = 1883
username = "mqtt"
password = "ICPHmqtt!"
client_id = "python_client_1"

lwt_topic = "status/last_will"
lwt_message = "Client disconnected unexpectedly"

def on_connect(client, userdata, flags, rc):
    print(f"Connected with result code {rc}")

def on_disconnect(client, userdata, rc):
    print(f"Disconnected with result code {rc}")

def on_log(client, userdata, level, buf):
    print(f"Log: {buf}")

client = mqtt.Client(client_id)
client.username_pw_set(username, password)

# Set the Last Will and Testament (LWT)
client.will_set(lwt_topic, lwt_message, qos=1, retain=False)

client.on_connect = on_connect
client.on_disconnect = on_disconnect
client.on_log = on_log

client.connect(broker, port, 60)
client.loop_start()

# Keep the client connected for 10 seconds
time.sleep(10)

# Simulate an unexpected disconnect by forcefully exiting
sys.exit()  # This will simulate the unexpected disconnect
