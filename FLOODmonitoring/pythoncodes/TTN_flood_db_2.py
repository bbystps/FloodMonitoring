import pymysql
import paho.mqtt.client as mqtt
import json
import time
import datetime
import pytz
import traceback
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Database connection function
def connect_to_db():
    try:
        conn = pymysql.connect(
            host='localhost',
            user='*****',
            password='*****',
            database='flood_monitoring')
        return conn
    except Exception as e:
        logger.error(f"Failed to connect to the database: {e}")
        return None

conn = connect_to_db()
cursor = conn.cursor() if conn else None

# Define callback functions
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        logger.info("Connected to MQTT broker")
        # client.subscribe("v3/test-app-868-2@ttn/devices/test-id/up")
        client.subscribe("v3/test-app-868-2@ttn/devices/lora-transceiver-2/up")
        
    else:
        logger.error(f"Failed to connect to MQTT broker with result code {rc}")

def on_message(client, userdata, message):
    msg = str(message.payload.decode("utf-8"))
    
    if message.topic == "v3/test-app-868-2@ttn/devices/test-id/up":
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
                logger.info(f"Battery Level: {BatteryLevel}, Water Level: {WaterLevel}, Device Status: {DeviceStatus}, RSSI: {RSSI}, SNR: {SNR}, Packet Rec. Ratio: {packet_rec_ratio:.2f}%")
                insert_data(WaterLevel, DeviceStatus, RSSI, SNR, packet_rec_ratio, BatteryLevel)
            else:
                logger.warning("Incomplete data received, skipping database insert.")
                
        except Exception as e:
            logger.error(f"An error occurred while processing MQTT message: {e}")
            traceback.print_exc()

def insert_data(WaterLevel, DeviceStatus, RSSI, SNR, packet_rec_ratio, BatteryLevel):
    global conn, cursor
    try:
        if conn is None or not conn.open:
            conn = connect_to_db()
            cursor = conn.cursor() if conn else None

        gmt8_time = datetime.datetime.now(pytz.timezone('Asia/Singapore'))
        TIMESTAMP = gmt8_time.strftime('%Y-%m-%d %H:%M:%S')
        query = """
            INSERT INTO brgy_nueva_era 
            (timestamp, water_level, device_status, rssi_val, snr, packet_rec_ratio, battery_voltage)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        cursor.execute(query, (TIMESTAMP, WaterLevel, DeviceStatus, RSSI, SNR, packet_rec_ratio, BatteryLevel))
        conn.commit()
        logger.info("Data inserted successfully into brgy_nueva_era")
    except Exception as e:
        logger.error(f"Database error: {e}")
        if conn:
            conn.rollback()
        traceback.print_exc()

# Create a client instance
client = mqtt.Client()

# Set callback functions
client.on_connect = on_connect
client.on_message = on_message

# Set username and password
username = "test-app-868-2@ttn"
password = "NNSXS.HVL3QSKQKKC4XPFVIDQQDWYY6A2RVOMT4NOSI4A.RMBWGGVEATQLWZVO2IWODYTIV5M6QJ6USK2CJZIRM3AOBHFSRWVQ"
client.username_pw_set(username, password)

# MQTT reconnect logic
while True:
    try:
        client.connect("eu1.cloud.thethings.network", 1883, 60)
        client.loop_forever()
    except Exception as e:
        logger.error(f"MQTT connection error: {e}")
        traceback.print_exc()
        time.sleep(5)  # Delay before retrying
