<?php

include("db_conn.php");

try {
    $pdo = new PDO("mysql:host=$host;dbname=$dbname", $username, $password);
    $pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);

    // Get the station name from the request
    $station = $_GET['station'];

    // Validate table name to prevent SQL injection
    if (!preg_match('/^[a-zA-Z0-9_]+$/', $station)) {
        throw new Exception("Invalid table name.");
    }

    // SQL to delete all data from the table
    $sql = "DELETE FROM `$station`";
    $stmt = $pdo->prepare($sql);
    $stmt->execute();

    // Return success response
    echo json_encode(['success' => true]);
} catch (PDOException $e) {
    echo json_encode(['success' => false, 'message' => $e->getMessage()]);
} catch (Exception $e) {
    echo json_encode(['success' => false, 'message' => $e->getMessage()]);
}
?>
