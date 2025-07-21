import asyncio
import websockets
import paho.mqtt.client as mqtt
import json
import base64

connected_clients = set()
event_loop = None  # Declare a global event loop

# WebSocket server to broadcast messages
async def broadcast_message(message):
    if connected_clients:
        # Gather all tasks to send messages to each WebSocket client
        await asyncio.gather(*(client.send(message) for client in connected_clients))

# Define MQTT callback functions
def on_connect(client, userdata, flags, rc):
    print("Connected to MQTT broker with result code", rc)
    client.subscribe("v3/test-app-868-2@ttn/devices/test-id/down/ack")

def on_message(client, userdata, message):
    msg = str(message.payload.decode("utf-8"))

    if message.topic == "v3/test-app-868-2@ttn/devices/test-id/down/ack":
        try:
            json_msg = json.loads(msg)
            print(json_msg)
            if 'downlink_ack' in json_msg:
                downlink_ack = json_msg['downlink_ack']
                confirmed = downlink_ack.get('confirmed', False)
                f_cnt = downlink_ack.get('f_cnt', None)
                frm_payload = downlink_ack.get('frm_payload', None)
                attempt = downlink_ack.get('confirmed_retry', {}).get('attempt', 1)

                if frm_payload:
                    decoded_payload = base64.b64decode(frm_payload).hex()
                else:
                    decoded_payload = "No payload"

                ack_message = {
                    "confirmed": confirmed,
                    "f_cnt": f_cnt,
                    "decoded_payload": decoded_payload,
                    "attempt": attempt
                }
                
                print(confirmed)
                
                # Schedule sending the message to WebSocket clients as a coroutine
                asyncio.run_coroutine_threadsafe(broadcast_message(json.dumps(ack_message)), event_loop)

        except Exception as e:
            print("Error processing message:", e)

# Start MQTT client
mqtt_client = mqtt.Client()
mqtt_client.on_connect = on_connect
mqtt_client.on_message = on_message
mqtt_client.username_pw_set("test-app-868-2@ttn", "NNSXS.HVL3QSKQKKC4XPFVIDQQDWYY6A2RVOMT4NOSI4A.RMBWGGVEATQLWZVO2IWODYTIV5M6QJ6USK2CJZIRM3AOBHFSRWVQ")
mqtt_client.connect("eu1.cloud.thethings.network", 1883, 60)

# WebSocket server
async def websocket_handler(websocket, path):
    connected_clients.add(websocket)
    try:
        async for message in websocket:
            pass  # No need to process incoming WebSocket messages in this example
    finally:
        connected_clients.remove(websocket)

# Start WebSocket server and MQTT client loop
async def main():
    global event_loop
    event_loop = asyncio.get_running_loop()  # Set the global event loop to the current running loop
    
    mqtt_client.loop_start()  # Start the MQTT loop in a separate thread
    server = await websockets.serve(websocket_handler, "0.0.0.0", 5001)
    print("WebSocket server started on ws://3.27.210.100:5001")
    await server.wait_closed()

# Run the main event loop
asyncio.run(main())