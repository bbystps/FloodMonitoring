import paho.mqtt.client as mqtt

# MQTT settings
broker = "3.27.210.100"
port = 1883
username = "mqtt"
password = "ICPHmqtt!"
client_id = "python_client_2"

# Status topic to listen to
status_topic = "status/last_will"

def on_connect(client, userdata, flags, rc):
    print(f"Connected with result code {rc}")
    client.subscribe(status_topic)
    print(f"Subscribed to topic: {status_topic}")

def on_message(client, userdata, msg):
    print(f"Message received on topic {msg.topic}: {msg.payload.decode()}")

def on_log(client, userdata, level, buf):
    print(f"Log: {buf}")

# Create a new MQTT client instance
client = mqtt.Client(client_id)

# Set username and password
client.username_pw_set(username, password)

# Assign callbacks
client.on_connect = on_connect
client.on_message = on_message
client.on_log = on_log

# Connect to the broker
client.connect(broker, port, 60)

# Start the network loop
client.loop_start()

# Keep the script running
try:
    while True:
        pass
except KeyboardInterrupt:
    print("Interrupted by user")
    client.loop_stop()
    client.disconnect()
