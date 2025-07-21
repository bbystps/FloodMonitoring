<?php
session_start();
$connect = new mysqli("localhost", "*****", "*****", "flood_monitoring");

if ($connect->connect_error) {
  die("Connection Failed: " . $connect->connect_error);
}
$username = $_POST["username"];
// echo json_encode($USERNAME);

if (!empty($username)) {
  $sql = "SELECT * FROM account WHERE username='$username' ";
  $result = $connect->query($sql);
  if ($result->num_rows > 0) {
    while ($row = $result->fetch_assoc()) {
      $password = $row["password"];
      if (password_verify($_POST['password'], $password)) {
        // Verification success! User has logged-in!
        // Create sessions, so we know the user is logged in, they basically act like cookies but remember the data on the server.
        session_regenerate_id();
        $_SESSION['loggedin'] = TRUE;
        $_SESSION['username'] = $_POST['username'];

        $myArr = new stdClass();
        $myArr->login = "SUCCESS";
        // $myArr -> privilege = $privilege;
        $myJSON = json_encode($myArr);
        echo $myJSON;
      } else {
        // Incorrect password
        $myArr = new stdClass();
        $myArr->login = "FAIL";
        $myJSON = json_encode($myArr);
        echo $myJSON;
      }
    }
  } else {
    $myArr = new stdClass();
    $myArr->login = "NO USER";
    $myJSON = json_encode($myArr);
    echo $myJSON;
  }
} else {
  $myArr = new stdClass();
  $myArr->login = "EMPTY FIELD";
  $myJSON = json_encode($myArr);
  echo $myJSON;
}
