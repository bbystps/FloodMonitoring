<?php
include("db_conn.php");

try {
    $pdo = new PDO("mysql:host=$host;dbname=$dbname", $username, $password);
    $pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);

    // Fetch the latest record for each sensor_id
    $stmt = $pdo->query("
        SELECT station, status, timestamp
        FROM station_status AS ss1
        WHERE timestamp = (
            SELECT MAX(timestamp)
            FROM station_status AS ss2
            WHERE ss1.station = ss2.station
        )
    ");
    
    $sensorData = $stmt->fetchAll(PDO::FETCH_ASSOC);

    // Initialize counters
    $totalDevices = 0;
    $offlineDevices = 0;
    $totalAlarms = 0;
    $needsMaintenance = 0;

    // Loop through the results and count based on conditions
    foreach ($sensorData as $sensor) {
        $totalDevices++;
        
        if ($sensor['status'] == 'Offline') {
            $offlineDevices++;
        }

        if ($sensor['status'] == 'alarms') {
            $totalAlarms++;
        }

        if ($sensor['status'] == 'maintenance') {
            $needsMaintenance++;
        }
    }

    // Prepare the response as an associative array
    $response = [
        'totalDevices' => $totalDevices,
        'offlineDevices' => $offlineDevices,
        'totalAlarms' => $totalAlarms,
        'needsMaintenance' => $needsMaintenance
    ];

    // Return the response as JSON
    echo json_encode($response);

} catch (PDOException $e) {
    echo 'Connection failed: ' . $e->getMessage();
}
?>