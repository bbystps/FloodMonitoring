<?php
include("db_conn.php");

try {
    $pdo = new PDO("mysql:host=$host;dbname=$dbname", $username, $password);
    $pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);

    $Station = isset($_GET['Station']) ? $_GET['Station'] : '';
    // $Station = "Station 2";

    // Prepare the query with a parameterized statement
    $stmt = $pdo->prepare("SELECT location, sensor_id FROM site_locations WHERE station = :station");
    $stmt->execute(['station' => $Station]);

    // Fetch unique site locations
    $fetchedData = $stmt->fetchAll(PDO::FETCH_ASSOC);

    // Return as JSON
    echo json_encode($fetchedData);
} catch (PDOException $e) {
    echo 'Connection failed: ' . $e->getMessage();
}
?>