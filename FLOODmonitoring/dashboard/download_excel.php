<?php

include("db_conn.php");

try {
    $pdo = new PDO("mysql:host=$host;dbname=$dbname", $username, $password);
    $pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);

    // $station = "brgy_nueva_era";
    $station = $_GET['station'];
    
    // Construct the SQL query with the validated station name
    $sql = "SELECT water_level, rssi_val, snr, packet_rec_ratio, battery_voltage, timestamp FROM `$station`"; // Using backticks to enclose the table name
    $stmt = $pdo->prepare($sql);
    $stmt->execute();

    $data = $stmt->fetchAll(PDO::FETCH_ASSOC);

    echo json_encode($data);
} catch (PDOException $e) {
    die("Connection failed: " . $e->getMessage());
} catch (Exception $e) {
    die("Error: " . $e->getMessage());
}
?>
