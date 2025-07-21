import pymysql
import paho.mqtt.client as mqtt
import json
import datetime
import pytz
import time

# Connect to the MySQL database
conn = pymysql.connect(
    host='localhost',
    user='*****',
    password='*****',
    database='rht_monitoring'
)

# Create a cursor object
cursor = conn.cursor()


# Global threshold variables
TEMP_LOW_THRESHOLD = 18.0  # Example default low threshold for temperature
TEMP_HIGH_THRESHOLD = 30.0  # Example default high threshold for temperature
HUMIDITY_LOW_THRESHOLD = 30.0  # Example default low threshold for humidity
HUMIDITY_HIGH_THRESHOLD = 70.0  # Example default high threshold for humidity

def get_thresholds():
    global TEMP_LOW_THRESHOLD, TEMP_HIGH_THRESHOLD, HUMIDITY_LOW_THRESHOLD, HUMIDITY_HIGH_THRESHOLD
    try:
        cursor.execute("SELECT temp_low, temp_high, hum_low, hum_high FROM threshold LIMIT 1")
        result = cursor.fetchone()
        if result:
            # Convert the fetched values to float to ensure proper comparisons
            TEMP_LOW_THRESHOLD = float(result[0])
            TEMP_HIGH_THRESHOLD = float(result[1])
            HUMIDITY_LOW_THRESHOLD = float(result[2])
            HUMIDITY_HIGH_THRESHOLD = float(result[3])
            print(f"temp low: {TEMP_LOW_THRESHOLD}")
            print(f"temp high: {TEMP_HIGH_THRESHOLD}")
            print(f"hum low: {HUMIDITY_LOW_THRESHOLD}")
            print(f"hum high: {HUMIDITY_HIGH_THRESHOLD}")

            print(f"Thresholds updated from database: Temp Low: {TEMP_LOW_THRESHOLD}, Temp High: {TEMP_HIGH_THRESHOLD}, "
                  f"Humidity Low: {HUMIDITY_LOW_THRESHOLD}, Humidity High: {HUMIDITY_HIGH_THRESHOLD}")
        else:
            print("No threshold data found in the database.")
    except Exception as e:
        print(f"Error fetching thresholds: {e}")


get_thresholds()

# Define callback functions
def on_connect(client, userdata, flags, rc):
    print(f"Connected with result code {rc}")
    client.subscribe("RHT/#")  # Subscribe to all topics under "RHT/#"
    client.subscribe("change_threshold") 
    print("Subscribed to topic RHT/#")

def on_message(client, userdata, message):
    print(f"Message received on topic: {message.topic}")
    get_thresholds()
    if message.topic == "change_threshold":
        # Call get_thresholds to update thresholds from the database
        print("Received change_threshold message, updating thresholds...")
        get_thresholds()
    else:
        msg_main = str(message.payload.decode("utf-8"))
        print(f"Received message on topic {message.topic}: {msg_main}")
        process_data(message.topic, msg_main)

def process_data(topic, msg_main):
    try:
        json_msg_main = json.loads(msg_main)
        temperature = float(json_msg_main["TEMP"])  # Convert to float and adjust
        humidity = float(json_msg_main["RH"])  # Convert to float
        smoke_detector = json_msg_main["SD"]
        status = json_msg_main["STAT"]

        print(f"Temperature: {temperature}, Humidity: {humidity}")

        # Check if temperature or humidity is out of range
        if temperature < TEMP_LOW_THRESHOLD:
            alarm_msg = "Temperature below threshold level: " + str(temperature) + "°C"
            print(f"{alarm_msg}")
            log_alarm(topic, alarm_msg)
        if temperature > TEMP_HIGH_THRESHOLD:
            alarm_msg = "Temperature above threshold level: " + str(temperature) + "°C"
            print(f"{alarm_msg}")
            log_alarm(topic, alarm_msg)
        if humidity < HUMIDITY_LOW_THRESHOLD:
            alarm_msg = "Humidity below threshold level: " + str(humidity) + "°C"
            print(f"{alarm_msg}")
            log_alarm(topic, alarm_msg)
        if humidity > HUMIDITY_HIGH_THRESHOLD:
            alarm_msg = "Humidity above threshold level: " + str(humidity) + "°C"
            print(f"{alarm_msg}")
            log_alarm(topic, alarm_msg)

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
    # print("Current time in GMT+8:", timestamp)

    try:
        sensor_id = topic.split('/')[-1]
        db_region = sensor_id.split('-')[0].lower()
        # print(f"Saving data of: {sensor_id}, region: {db_region}")

        insert_query = f"INSERT INTO {db_region} (sensor_id, temperature, humidity, smoke_detector, status, timestamp) VALUES (%s, %s, %s, %s, %s, %s)"
        cursor.execute(insert_query, (sensor_id, temperature, humidity, smoke_detector, status, timestamp))
        conn.commit()

        print(f"Insert Success for {sensor_id}")
    except Exception as e:
        print("An error occurred:", e)
        conn.rollback()

def log_alarm(topic, alarm_msg):
    gmt8_time = datetime.datetime.now(pytz.timezone('Asia/Singapore'))
    timestamp = gmt8_time.strftime('%Y-%m-%d %H:%M:%S')

    try:
        sensor_id = topic.split('/')[-1]
        print(f"Log Alarm Here")
        db_region_logs = sensor_id.split('-')[0].lower() + "_alarm_logs"
        print(f"Saving data of: {sensor_id}, region: {db_region_logs}")

        alarm_query = f"INSERT INTO {db_region_logs} (sensor_id, logs, timestamp) VALUES (%s, %s, %s)"
        cursor.execute(alarm_query, (sensor_id, alarm_msg, timestamp))
        conn.commit()

        print(f"Alarm logged for {sensor_id}: {alarm_msg}")
    except Exception as e:
        print("An error occurred while logging the alarm:", e)
        conn.rollback()

# MQTT Client setup
client = mqtt.Client()

# Set callback functions
client.on_connect = on_connect
client.on_message = on_message

# Set username and password
username = "*****"
password = "*****"
client.username_pw_set(username, password)

# Connect to the MQTT broker
client.connect("3.27.210.100", 1883, 60)

# Start the MQTT client loop
client.loop_forever()
