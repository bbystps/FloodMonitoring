<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>LoRaWAN Acknowledgments</title>
</head>
<body>
    <div id="status">Connecting...</div>
    <div id="messages"></div>

    <script>
        const ws = new WebSocket("ws://localhost:6789");

        ws.onopen = () => {
            console.log("WebSocket connected");
            document.getElementById("status").innerText = "WebSocket connected.";
        };

        ws.onmessage = (event) => {
            // Parse incoming message and display it
            try {
                const data = JSON.parse(event.data);
                const messageDiv = document.createElement("div");
                messageDiv.innerText = `Acknowledgment - Confirmed: ${data.confirmed}, Frame Count: ${data.f_cnt}, Payload: ${data.decoded_payload}, Attempt: ${data.attempt}`;
                document.getElementById("messages").appendChild(messageDiv);
                if (data.confirmed == "true"){
                    console.log("TRUE");
                }
            } catch (error) {
                console.error("Error parsing WebSocket message:", error);
            }
        };

        ws.onerror = (error) => {
            console.error("WebSocket error:", error);
            document.getElementById("status").innerText = "WebSocket error.";
        };

        ws.onclose = () => {
            console.log("WebSocket closed");
            document.getElementById("status").innerText = "WebSocket disconnected.";
        };
    </script>
</body>
</html>
