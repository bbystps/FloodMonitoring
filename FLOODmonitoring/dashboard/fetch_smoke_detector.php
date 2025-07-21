<?php
include("db_conn.php");

try {
  $pdo = new PDO("mysql:host=$host;dbname=$dbname", $username, $password);
  $pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);

  // Get sensor_id from the request
  $station = $_GET['station'];
  // $station = "brgy_nueva_era";

  // Fetch the smoke detector value for the specific sensor_id
  $stmt = $pdo->prepare("SELECT water_level FROM $station ORDER BY id DESC LIMIT 1");
  $stmt->execute();
  $result = $stmt->fetch(PDO::FETCH_ASSOC);

  $response = [
    'water_level' => $result['water_level'],
  ];

  // Return the response as JSON
  header('Content-Type: application/json');
  echo json_encode($response);
  
} catch (PDOException $e) {
  echo json_encode(['error' => 'Connection failed: ' . $e->getMessage()]);
}
?>
