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
    $name = htmlspecialchars($name, ENT_QUOTES, 'UTF-8');
    $name = preg_replace('/[^a-zA-Z0-9_ ]/', '', $name);
    $name = ucwords(strtolower($name));
    return trim($name);
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

$person = new Login;
$person->setUsername($_POST["username"]);
$person->setPassword($_POST["password"]);

if ($_SESSION['user'] != $person->getUsername()) {
    echo "User is logged in with different username and password";
    return;
}

// Fetch user details by username
$query = "SELECT * FROM login WHERE username='" . $person->getUsername() . "'";
$result = mysqli_query($con, $query);
$numrows = mysqli_num_rows($result);

if ($numrows == 0) {
    echo "Error : Username is incorrect";
    exit();
}

// Get the stored hashed password
$row = mysqli_fetch_assoc($result);
$hashedPassword = $row['password'];

// Verify the input password with the hashed password
if (!password_verify($person->getPassword(), $hashedPassword)) {
    echo "Error : Password is incorrect";
    exit();
}

if (!isValidCoordinate($_POST["latitude"], 'latitude') || !isValidCoordinate($_POST["longitude"], 'longitude')) {
    echo "Error : Check Latitude and Longitude Value";
    exit();
}

if (!isStringNumber($_POST["demand"])) {
    echo "Error : Check Demand Value";
    exit();
}

$district = $_POST["district"];
$latitude = $_POST["latitude"];
$longitude = $_POST["longitude"];
$name = formatName($_POST["name"]);
$id = formatName($_POST["id"]);
$type = $_POST["type"];
$demand = $_POST["demand"];
$uniqueid = uniqid("FPS_",);

$FPS = new FPS;
$FPS->setUniqueid(substr($uniqueid, 0, 15));
$FPS->setDistrict(ucwords(strtolower($district)));
$FPS->setLatitude($latitude);
$FPS->setLongitude($longitude);
$FPS->setName($name);
$FPS->setId($id);
$FPS->setType($type);
$FPS->setDemand($demand);
$FPS->setActive("1");

$query_insert_check = $FPS->checkInsert($FPS);
$query_insert_result = mysqli_query($con, $query_insert_check);
$numrows_insert = mysqli_num_rows($query_insert_result);

if ($numrows_insert == 0) {
    $query = $FPS->insert($FPS);
    mysqli_query($con, $query);
    mysqli_close($con);
    echo "<script>window.location.href = '../FPS.php';</script>";
} else {
    echo "Error : Error in Insertion as FPS id already exists";
}

?>
<?php require('Fullui.php'); ?>
