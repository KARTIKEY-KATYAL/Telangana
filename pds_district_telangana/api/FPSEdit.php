<?php

require('../util/Connection.php');
require('../structures/FPS.php');
require('../util/SessionFunction.php');
require('../structures/Login.php');

if (!SessionCheck()) {
    return;
}

require('Header.php');

function formatName($name) {
    $name = preg_replace('/[^a-zA-Z ]/', '', $name);
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

// Create a new Login object and set credentials
$person = new Login;
$person->setUsername($_POST["username"]);
$person->setPassword($_POST["password"]);

// Check if the session username matches
if ($_SESSION['district_user'] != $person->getUsername()) {
    echo "User is logged in with a different username and password.";
    return;
}

// Fetch user data securely
$query = "SELECT * FROM login WHERE username='" . mysqli_real_escape_string($con, $person->getUsername()) . "'";
$result = mysqli_query($con, $query);
$numrows = mysqli_num_rows($result);

if ($numrows == 0) {
    echo "Error: Username does not exist.";
    return;
}

$row = mysqli_fetch_assoc($result);

// Use password_verify to check the hashed password
if (!password_verify($person->getPassword(), $row['password'])) {
    echo "Error: Password is incorrect.";
    return;
}

// Validate latitude and longitude
if (!isValidCoordinate($_POST["latitude"], 'latitude') || !isValidCoordinate($_POST["longitude"], 'longitude')) {
    echo "Error: Check Latitude and Longitude Value.";
    exit();
}

// Validate demand value
if (!isStringNumber($_POST["demand"])) {
    echo "Error: Check Demand Value.";
    exit();
}

$district = formatName($_POST["district"]);
$latitude = $_POST["latitude"];
$longitude = $_POST["longitude"];
$name = formatName($_POST["name"]);
$id = $_POST["id"];
$type = $_POST["type"];
$demand = $_POST["demand"];
$uniqueid = $_POST["uniqueid"];
$active = $_POST["active"];

// Create a new FPS object and set its properties
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

// Prepare the update query
$query = $FPS->update($FPS);
mysqli_query($con, $query);

// Close the database connection
mysqli_close($con);

// Redirect to the FPS page
echo "<script>window.location.href = '../FPS.php';</script>";
?>
<?php require('Fullui.php'); ?>
