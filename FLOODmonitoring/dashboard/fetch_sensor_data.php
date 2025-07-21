<?php
include("db_conn.php");

// Sanitize the input
$regionDB = isset($_GET['regionDB']) ? preg_replace('/[^a-zA-Z0-9_]/', '', $_GET['regionDB']) : '';

try {
  $pdo = new PDO("mysql:host=$host;dbname=$dbname", $username, $password);
  $pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);

  // Prepare and execute SQL statement
  $stmt = $pdo->prepare("
    SELECT * FROM $regionDB
    WHERE id IN (
        SELECT MAX(id) FROM $regionDB
        GROUP BY sensor_id
    )
    ORDER BY id DESC
  ");
  $stmt->execute();
  $siteLocations = $stmt->fetchAll(PDO::FETCH_ASSOC);

  // Return as JSON
  echo json_encode($siteLocations);

} catch (PDOException $e) {
  // Return an error message in JSON format
  echo json_encode(['error' => 'Connection failed: ' . $e->getMessage()]);
}
?>
