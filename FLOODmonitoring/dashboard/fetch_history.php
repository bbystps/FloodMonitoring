<?php

include("db_conn.php");

try {
    $pdo = new PDO("mysql:host=$host;dbname=$dbname", $username, $password);
    $pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);

    // $station = "brgy_nueva_era";
    // $station = $_GET['station'];
    
    $station = isset($_GET['station']) ? $_GET['station'] : '';
    $last_id = isset($_GET['last_id']) ? intval($_GET['last_id']) : 0;

    
    // Check for invalid sensor_id
    if (empty($station)) {
        echo json_encode(["error" => "Invalid sensor_id"]);
        exit;
    }

    // Verify that the table exists
    $stmt = $pdo->prepare("SHOW TABLES LIKE :station");
    $stmt->bindParam(':station', $station, PDO::PARAM_STR);
    $stmt->execute();
    if ($stmt->rowCount() === 0) {
        echo json_encode(["error" => "Table does not exist"]);
        exit;
    }

    // Fetch data 
    $sql = "SELECT id, water_level, timestamp FROM `$station` WHERE id > :last_id ORDER BY id DESC LIMIT 20";
    $stmt = $pdo->prepare($sql);
    $stmt->bindParam(':last_id', $last_id, PDO::PARAM_INT);
    $stmt->execute();

    $data = $stmt->fetchAll(PDO::FETCH_ASSOC);

    // Reverse the data to make it ascending for the chart
    $data = array_reverse($data);
    echo json_encode($data);
} catch (PDOException $e) {
    echo json_encode(["error" => "Connection failed: " . $e->getMessage()]);
}

//     // Construct the SQL query with the validated station name
//     $sql = "SELECT water_level, timestamp FROM `$station` ORDER BY id DESC LIMIT 20"; // Using backticks to enclose the table name
//     $stmt = $pdo->prepare($sql);
//     $stmt->execute();

//     $data = $stmt->fetchAll(PDO::FETCH_ASSOC);

//     echo json_encode($data);
// } catch (PDOException $e) {
//     die("Connection failed: " . $e->getMessage());
// } catch (Exception $e) {
//     die("Error: " . $e->getMessage());
// }
?>
