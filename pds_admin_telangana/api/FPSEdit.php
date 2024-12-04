<?php

require('../util/Connection.php');
require('../structures/FPS.php');
require('../util/SessionFunction.php');
require('../structures/Login.php');

// Check if session is valid
if (!SessionCheck()) {
    return;
}

require('Header.php');

function formatName($name) {
    $name = htmlspecialchars($name, ENT_QUOTES, 'UTF-8');
    $name = preg_replace('/[^a-zA-Z0-9_ ]/', '', $name);
    return trim(ucwords(strtolower($name)));
}

function isValidCoordinate($value, $coordinateType) {
    if (!is_numeric($value)) {
        return false;
    }

    $coordinate = floatval($value);

    switch ($coordinateType) {
        case 'latitude':
            return ($coordinate >= -90 && $coordinate <= 90);
        case 'longitude':
            return ($coordinate >= -180 && $coordinate <= 180);
        default:
            return false;
    }
}

function isStringNumber($stringValue) {
    return is_numeric($stringValue);
}

// Create Login object and set credentials
$person = new Login;
$person->setUsername($_POST["username"]);
$person->setPassword($_POST["password"]);

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

// Validate latitude and longitude
if (!isValidCoordinate($_POST["latitude"], 'latitude') || !isValidCoordinate($_POST["longitude"], 'longitude')) {
    echo "Error: Check Latitude and Longitude Value";
    exit();
}

// Validate demand value
if (!isStringNumber($_POST["demand"])) {
    echo "Error: Check Demand Value";
    exit();
}

// Sanitize and set the FPS details
$district = formatName($_POST["district"]);
$latitude = $_POST["latitude"];
$longitude = $_POST["longitude"];
$name = formatName($_POST["name"]);
$id = formatName($_POST["id"]);
$type = $_POST["type"];
$demand = $_POST["demand"];
$uniqueid = $_POST["uniqueid"];
$active = $_POST["active"];

$FPS = new FPS;
$FPS->setUniqueid($uniqueid);
$FPS->setDistrict($district);
$FPS->setLatitude($latitude);
$FPS->setLongitude($longitude);
$FPS->setName($name);
$FPS->setId($id);
$FPS->setType($type);
$FPS->setDemand($demand);
$FPS->setActive($active);

// Check if FPS ID already exists
$query_check = $FPS->checkInsert($FPS);
$query_result = mysqli_query($con, $query_check);
$numrows = mysqli_num_rows($query_result);
if ($numrows != 0) {
    $row = mysqli_fetch_assoc($query_result);
    $uniqueid_check = $row["uniqueid"];
    if ($uniqueid != $uniqueid_check) {
        echo "Error: In updating data as FPS id already exists ID: " . $id;
        echo "</br>";
        exit();
    }
}

// Update the FPS record
$query = $FPS->update($FPS);
mysqli_query($con, $query);

// Close the database connection
mysqli_close($con);

// Redirect to FPS.php
echo "<script>window.location.href = '../FPS.php';</script>";

?>

<?php require('Fullui.php'); ?>
