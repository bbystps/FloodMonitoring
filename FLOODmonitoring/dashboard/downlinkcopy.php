<?php
$tenant = "eu1";
$application_id = "test-app-868-2";
$device_id = "test-id";
$api_key = "NNSXS.AEHDZGODV7M4MFDCQ442VHRZTSI4AZP2CSFM6YA.HUIMH3QYCORCHY5RYATQQJ4N5A7V3T24PXEYVD3PBUZYFTBWXO4A";

// Construct the URL for the HTTP request
$url = "https://{$tenant}.cloud.thethings.network/api/v3/as/applications/{$application_id}/devices/{$device_id}/down/push";

// Check if the request is POST and contains 'payload'
if ($_SERVER['REQUEST_METHOD'] === 'POST' && isset($_POST['payload'])) {
    // Get the payload from the AJAX request
    $input_payload = $_POST['payload'];

    // Convert payload to binary (e.g., '01' => 0x01)
    $binary_payload = pack('C*', hexdec($input_payload));

    // Base64 encode the payload
    $encoded_payload = base64_encode($binary_payload);

    // Payload to send downlink with confirmed flag
    $payload_with_params = [
        "downlinks" => [
            [
                "frm_payload" => $encoded_payload,
                "f_port" => 1,  // The port your device is listening on
                "confirmed" => true,  // Enable confirmed downlink
                "priority" => "NORMAL"
            ]
        ]
    ];

    // Set the headers for the API request
    $headers = [
        "Authorization: Bearer {$api_key}",
        "Content-Type: application/json"
    ];

    // Function to send the downlink via cURL
    $ch = curl_init($url);
    
    // Encode the payload as JSON
    $json_payload = json_encode($payload_with_params);
    
    // Set cURL options
    curl_setopt($ch, CURLOPT_POST, 1);
    curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);
    curl_setopt($ch, CURLOPT_HTTPHEADER, $headers);
    curl_setopt($ch, CURLOPT_POSTFIELDS, $json_payload);
    
    // Execute the cURL request
    $response = curl_exec($ch);
    
    // Check if the request was successful
    if(curl_errno($ch)) {
        echo 'cURL Error: ' . curl_error($ch);
    } else {
        $http_code = curl_getinfo($ch, CURLINFO_HTTP_CODE);
        if ($http_code == 200) {
            echo "Downlink sent successfully!";
        } else {
            echo "Failed to send downlink. HTTP Status Code: $http_code\n";
            echo "Response: $response";
        }
    }

    // Close the cURL session
    curl_close($ch);
} else {
    echo "Invalid request.";
}
?>