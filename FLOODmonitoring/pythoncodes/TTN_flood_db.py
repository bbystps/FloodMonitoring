import pymysql
import paho.mqtt.client as mqtt
import json
import time
import datetime
import pytz
import traceback
import logging
import threading

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Database connection function
def connect_to_db():
    try:
        conn = pymysql.connect(
            host='localhost',
            user='root',
            password='ICPHpass!',
            database='flood_monitoring')
        return conn
    except Exception as e:
        logger.error(f"Failed to connect to the database: {e}")
        return None

# Initialize database connection
conn = connect_to_db()
cursor = conn.cursor() if conn else None

# Initialize web MQTT client for website notifications
web_mqtt_client = mqtt.Client()
web_mqtt_username = "mqtt"
web_mqtt_password = "ICPHmqtt!"
web_mqtt_client.username_pw_set(web_mqtt_username, web_mqtt_password)

def on_web_disconnect(client, userdata, rc):
    logger.warning("Web MQTT client disconnected. Attempting to reconnect...")
    while True:
        try:
            client.reconnect()
            logger.info("Web MQTT client reconnected.")
            break
        except Exception as e:
            logger.error(f"Reconnection failed: {e}")
            time.sleep(5)

web_mqtt_client.on_disconnect = on_web_disconnect

try:
    web_mqtt_client.connect("3.27.210.100", 1883, 60)
    web_mqtt_client.loop_start()  # Start loop in the background to maintain the connection
    logger.info("Connected to the web MQTT broker.")
except Exception as e:
    logger.error(f"Failed to connect to the web MQTT broker: {e}")
    traceback.print_exc()

# Define callback functions for TTN MQTT client
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        logger.info("Connected to TTN MQTT broker.")
        client.subscribe("v3/test-app-868-2@ttn/devices/test-id/up")
        client.subscribe("v3/test-app-868-2@ttn/devices/lora-transceiver-2/up")
    else:
        logger.error(f"Failed to connect to TTN MQTT broker with result code {rc}")

def on_message(client, userdata, message):
    msg = str(message.payload.decode("utf-8"))
    try:
        json_msg = json.loads(msg)
        
        # Extract 'uplink_message' and 'rx_metadata'
        uplink_message = json_msg.get('uplink_message', {})
        rx_metadata = uplink_message.get('rx_metadata', [])
        rx_metadata = rx_metadata[0] if len(rx_metadata) > 0 else {}
        decoded_payload = uplink_message.get('decoded_payload', {})
        
        # Extract values
        WaterLevel = decoded_payload.get('WaterLevel')
        BatteryLevel = decoded_payload.get('BatteryLevel')
        DeviceStatus = decoded_payload.get('DeviceStatus')
        RSSI = rx_metadata.get('rssi')
        SNR = rx_metadata.get('snr')
        dev_eui = json_msg.get('end_device_ids', {}).get('dev_eui', '')
        
        # Compute elapsed time since monitoring started
        monitoring_start_time = datetime.datetime(2024, 12, 2, 0, 0, 0)  # Replace with actual start time
        current_time = datetime.datetime.now()
        elapsed_time_in_seconds = (current_time - monitoring_start_time).total_seconds()
        
        # Calculate total_expected_packets for a 5-minute interval
        interval_between_messages = 300  # 5 minutes in seconds
        total_expected_packets = elapsed_time_in_seconds // interval_between_messages
        
        # Compute packet_rec_ratio using f_cnt
        f_cnt = uplink_message.get('f_cnt', 0)
        packet_rec_ratio = (f_cnt / total_expected_packets) * 100 if total_expected_packets > 0 else 0 
        
        if None not in (WaterLevel, BatteryLevel, DeviceStatus, RSSI, SNR):
            logger.info(f"Device EUI: {dev_eui}, Battery Level: {BatteryLevel}, Water Level: {WaterLevel}, Device Status: {DeviceStatus}, RSSI: {RSSI}, SNR: {SNR}, Packet Rec. Ratio: {packet_rec_ratio:.2f}%")
            
            # Choose the table based on dev_eui
            if dev_eui == "70B3D57ED006CC29":
                insert_data("brgy_san_marcos", WaterLevel, DeviceStatus, RSSI, SNR, packet_rec_ratio, BatteryLevel)
            elif dev_eui == "70B3D57ED006B769":
                insert_data("brgy_nueva_era", WaterLevel, DeviceStatus, RSSI, SNR, packet_rec_ratio, BatteryLevel)
            else:
                logger.warning(f"Unknown device EUI: {dev_eui}, skipping database insert.")
        else:
            logger.warning("Incomplete data received, skipping database insert.")
            
    except Exception as e:
        logger.error(f"An error occurred while processing MQTT message: {e}")
        traceback.print_exc()

# Insert data into the database and notify the website
def insert_data(table_name, WaterLevel, DeviceStatus, RSSI, SNR, packet_rec_ratio, BatteryLevel):
    global conn, cursor
    try:
        if conn is None or not conn.open:
            conn = connect_to_db()
            cursor = conn.cursor() if conn else None

        gmt8_time = datetime.datetime.now(pytz.timezone('Asia/Singapore'))
        TIMESTAMP = gmt8_time.strftime('%Y-%m-%d %H:%M:%S')
        query = f"""
            INSERT INTO {table_name} 
            (timestamp, water_level, device_status, rssi_val, snr, packet_rec_ratio, battery_voltage)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        cursor.execute(query, (TIMESTAMP, WaterLevel, DeviceStatus, RSSI, SNR, packet_rec_ratio, BatteryLevel))
        conn.commit()
        logger.info(f"Data inserted successfully into {table_name}")
        
        # Notify the website about new data
        notification_message = f"New data inserted into {table_name} at {TIMESTAMP}"
        result = web_mqtt_client.publish("FLOOD/WebUpdate", notification_message)
        if result.rc == mqtt.MQTT_ERR_SUCCESS:
            logger.info("Notification sent to the website MQTT broker.")
        else:
            logger.error("Failed to send notification to the website MQTT broker.")
        
    except Exception as e:
        logger.error(f"Database error: {e}") 
        if conn:
            conn.rollback()
        traceback.print_exc()

# Create TTN MQTT client
ttn_mqtt_client = mqtt.Client()
ttn_mqtt_client.on_connect = on_connect
ttn_mqtt_client.on_message = on_message

# Set credentials for TTN MQTT broker
ttn_username = "test-app-868-2@ttn"
ttn_password = "NNSXS.HVL3QSKQKKC4XPFVIDQQDWYY6A2RVOMT4NOSI4A.RMBWGGVEATQLWZVO2IWODYTIV5M6QJ6USK2CJZIRM3AOBHFSRWVQ"
ttn_mqtt_client.username_pw_set(ttn_username, ttn_password)

# Start TTN MQTT client in a separate thread
def start_ttn_client():
    while True:
        try:
            ttn_mqtt_client.connect("eu1.cloud.thethings.network", 1883, 60)
            ttn_mqtt_client.loop_forever()
        except Exception as e:
            logger.error(f"MQTT connection error: {e}")
            traceback.print_exc()
            time.sleep(5)  # Retry after delay

ttn_thread = threading.Thread(target=start_ttn_client)
ttn_thread.start()
