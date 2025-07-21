import pymysql
import paho.mqtt.client as mqtt
import json
import datetime
import pytz
import time

# Connect to the MySQL database
conn = pymysql.connect(
    host='localhost',
    user='root',
    password='',
    database='rht_monitoring'
)

# Create a cursor object
cursor = conn.cursor()

# Dictionary to store the last received timestamp for each topic
#last_received = {}

# Define callback functions
def on_connect(client, userdata, flags, rc):
    print(f"Connected with result code {rc}")
    client.subscribe("RHT/#")  # Subscribe to all topics under "p003-dyn"
    print("Subscribed to topic RHT/#")

def on_message(client, userdata, message):
    print(f"Message received on topic: {message.topic}")  # Debugging statement
    msg_main = str(message.payload.decode("utf-8"))
    print(f"Received message on topic {message.topic}: {msg_main}")
    process_data(message.topic, msg_main)

def process_data(topic, msg_main):
    try:
        json_msg_main = json.loads(msg_main)
        temperature = float(json_msg_main["TEMP"]) - 12
        humidity = json_msg_main["RH"]
        smoke_detector = json_msg_main["SD"]
        status = json_msg_main["STAT"]
        print(temperature)
        insert_data(topic, temperature, humidity, smoke_detector, status)
        
    except json.JSONDecodeError as e:
        print(f"Failed to decode JSON: {e}")
    except KeyError as e:
        print(f"Missing key in JSON data: {e}")
    except TypeError as e:
        print(f"Unexpected data type: {e}")

def insert_data(topic, temperature, humidity, smoke_detector, status):
    gmt8_time = datetime.datetime.now(pytz.timezone('Asia/Singapore'))
    timestamp = gmt8_time.strftime('%Y-%m-%d %H:%M:%S')
    print("Current time in GMT+8:", timestamp)

    try:
        sensor_id = topic.split('/')[-1]
        #sensor_id = f"METRICS{sid}"
        print(f"save data of: {sensor_id}")
        #topic_r = f"RHT/r/{sid}"
        db_region = sensor_id.split('-')[0].lower()
        print(f"region: {db_region}")

        insert_query = f"INSERT INTO {db_region} (sensor_id, temperature, humidity, smoke_detector, status, timestamp) VALUES (%s, %s, %s, %s, %s, %s)"
        cursor.execute(insert_query, (sensor_id, temperature, humidity, smoke_detector, status, timestamp))
        
        conn.commit()

        #client.publish("P003_dyn_ss", "SUCCESS")
        #client.publish(topic_r, "SUCCESS")
        print(f"Insert Success for {sensor_id}")
    except Exception as e:
        print("An error occurred:", e)
        conn.rollback()

client = mqtt.Client()

# Set callback functions
client.on_connect = on_connect
client.on_message = on_message

# Set username and password
username = "mqtt"
password = "ICPHmqtt!"
client.username_pw_set(username, password)

# Connect to the MQTT broker
client.connect("3.27.210.100", 1883, 60)

# Start the MQTT client loop
client.loop_forever()  # Use loop_forever to keep the script running
