<?php

require('../util/Connection.php');
require('../structures/MlspWarehouse.php');
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
    // Check if the value is a number
    if (!is_numeric($value)) {
        return false;
    }

    // Convert to float
    $coordinate = floatval($value);

    // Validate latitude and longitude
    return ($coordinateType === 'latitude' && $coordinate >= -90 && $coordinate <= 90) ||
           ($coordinateType === 'longitude' && $coordinate >= -180 && $coordinate <= 180);
}

function isStringNumber($stringValue) {
    return is_numeric($stringValue);
}

// Create a new Login object and set credentials
$person = new Login;
$person->setUsername($_POST["username"]);
$person->setPassword($_POST["password"]);

// Check if the logged-in user matches the session user
if ($_SESSION['district_user'] != $person->getUsername()) {
    echo "User is logged in with a different username and password";
    return;
}

// Fetch user data without exposing the password
$query = "SELECT * FROM login WHERE username='" . mysqli_real_escape_string($con, $person->getUsername()) . "'";
$result = mysqli_query($con, $query);
$numrows = mysqli_num_rows($result);

if ($numrows == 0) {
    echo "Error: Username does not exist.";
    return;
}

$row = mysqli_fetch_assoc($result);

// Verify password against the stored hashed password
if (!password_verify($person->getPassword(), $row['password'])) {
    echo "Error: Password is incorrect.";
    return;
}

// Validate latitude and longitude
if (!isValidCoordinate($_POST["latitude"], 'latitude') || !isValidCoordinate($_POST["longitude"], 'longitude')) {
    echo "Error: Check Latitude and Longitude Value.";
    exit();
}

// Validate storage value
if (!isStringNumber($_POST["storage"])) {
    echo "Error: Check Storage Value.";
    exit();
}

// Prepare and sanitize input data
$district = formatName($_POST["district"]);
$latitude = $_POST["latitude"];
$longitude = $_POST["longitude"];
$name = formatName($_POST["name"]);
$id = $_POST["id"];
$type = $_POST["type"];
$storage = $_POST["storage"];
$warehousetype = $_POST["warehousetype"];
$uniqueid = $_POST["uniqueid"];
$active = $_POST["active"];

$Warehouse = new MlspWarehouse;
$Warehouse->setUniqueid($uniqueid);
$Warehouse->setDistrict($district);
$Warehouse->setLatitude($latitude);
$Warehouse->setLongitude($longitude);
$Warehouse->setName($name);
$Warehouse->setId($id);
$Warehouse->setType($type);
$Warehouse->setStorage($storage);
$Warehouse->setWarehousetype($warehousetype);
$Warehouse->setActive($active);

// Prepare the update query
$query = $Warehouse->update($Warehouse);

// Execute the update
if (mysqli_query($con, $query)) {
    // Redirect on success
    echo "<script>window.location.href = '../MlspWarehouse.php';</script>";
} else {
    echo "Error: Unable to update warehouse data.";
}

// Close the database connection
mysqli_close($con);
?>
<?php require('Fullui.php'); ?>
