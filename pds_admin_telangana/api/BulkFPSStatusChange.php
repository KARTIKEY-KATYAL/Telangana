<?php

require('../util/Connection.php');
require('../structures/Warehouse.php');
require('../util/SessionFunction.php');
require('../structures/Login.php');

// Check if session is valid
if (!SessionCheck()) {
    return;
}

require('Header.php');

function sanitizeInput($input) {
    return htmlspecialchars(trim($input), ENT_QUOTES, 'UTF-8');
}

// Create Login object and set credentials
$person = new Login;
$person->setUsername(sanitizeInput($_POST["username"]));
$person->setPassword(sanitizeInput($_POST["password"]));

// Check if the current session user matches the one trying to perform the action
if ($_SESSION['user'] != $person->getUsername()) {
    echo "User is logged in with a different username and password.";
    return;
}

// Use prepared statements to prevent SQL injection
$query = "SELECT * FROM login WHERE username=?";
$stmt = mysqli_prepare($con, $query);
mysqli_stmt_bind_param($stmt, "s", $person->getUsername());
mysqli_stmt_execute($stmt);
$result = mysqli_stmt_get_result($stmt);
$numrows = mysqli_num_rows($result);

if ($numrows == 0) {
    echo "Error: Password or Username is incorrect.";
    return;
}

// Fetch user data and verify the password
$row = mysqli_fetch_assoc($result);
$hashedPassword = $row['password'];

if (!password_verify($person->getPassword(), $hashedPassword)) {
    echo "Error: Password or Username is incorrect.";
    return;
}

// Sanitize district, status, and fpstype inputs
$district = sanitizeInput($_POST["district"]);
$status = sanitizeInput($_POST["status"]);
$fpstype = sanitizeInput($_POST["fpstype"]);

// Prepare the update query based on the fps type and status
if ($fpstype == 'Model FPS') {
    $activeValue = ($status == 'active') ? '1' : '0';
    $query = "UPDATE fps SET active=? WHERE district=? AND type='Model FPS'";
} else {
    $activeValue = ($status == 'active') ? '1' : '0';
    $query = "UPDATE fps SET active=? WHERE district=? AND type='Normal FPS'";
}

// Use prepared statements for the update
$stmt = mysqli_prepare($con, $query);
mysqli_stmt_bind_param($stmt, "is", $activeValue, $district);
mysqli_stmt_execute($stmt);

// Redirect to FPS.php
echo "<script>window.location.href = '../FPS.php';</script>";

// Close the database connection
mysqli_close($con);
?>

<?php require('Fullui.php'); ?>
