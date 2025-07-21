<?php

include("db_conn.php");

try {
    $pdo = new PDO("mysql:host=$host;dbname=$dbname", $username, $password);
    $pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);

    // Check if sensor_id is provided
    $sensorId = isset($_GET['sensor_id']) ? $_GET['sensor_id'] : null;
    // $sensorId = "s1bne";

    // Fetch data only from today's date
    $sql = "SELECT * FROM `$sensorId` WHERE DATE(`timestamp`) = CURDATE() ORDER BY id DESC LIMIT 20";
    $stmt = $pdo->prepare($sql);
    
    $stmt->execute();
    $data = $stmt->fetchAll(PDO::FETCH_ASSOC);
    
    echo json_encode($data);

} catch (PDOException $e) {
    die("Connection failed: " . $e->getMessage());
}
?>
