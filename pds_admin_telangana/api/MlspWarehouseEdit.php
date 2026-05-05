<?php

require('../util/Connection.php');
require('../structures/MlspWarehouse.php');
require('../util/SessionFunction.php');
require('../structures/Login.php');

if(!SessionCheck()){
    return;
}

require('Header.php');

// Function to format the name securely
function formatName($name) {
    $name = htmlspecialchars($name, ENT_QUOTES, 'UTF-8');
    $name = preg_replace('/[^a-zA-Z0-9_ ]/', '', $name);
    $name = ucwords(strtolower($name));
    return trim($name);
}

// Function to validate coordinates (latitude/longitude)
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

// Function to check if a string is numeric
function isStringNumber($stringValue) {
    return is_numeric($stringValue);
}

// Initialize the Login object
$person = new Login;
$person->setUsername($_POST["username"]);
$person->setPassword($_POST["password"]);

// Check if the session user matches the provided username
if ($_SESSION['user'] != $person->getUsername()) {
    echo "User is logged in with a different username and password";
    return;
}

// Query to retrieve the user details based on username
$query = "SELECT * FROM login WHERE username='" . $person->getUsername() . "'";
$result = mysqli_query($con, $query);
$row = mysqli_fetch_assoc($result);

// Verify the entered password with the hashed password from the database
if ($row) {
    if (password_verify($person->getPassword(), $row['password'])) {
        // Password is correct, proceed with updating warehouse information
        
        // Validate coordinates and storage value
        if (!isValidCoordinate($_POST["latitude"], 'latitude') || !isValidCoordinate($_POST["longitude"], 'longitude')) {
            echo "Error: Check Latitude and Longitude Value";
            exit();
        }

        if (!isStringNumber($_POST["storage"])) {
            echo "Error: Check Storage Value";
            exit();
        }

        // Collect and format the input data
        $district = formatName($_POST["district"]);
        $latitude = $_POST["latitude"];
        $longitude = $_POST["longitude"];
        $name = formatName($_POST["name"]);
        $id = formatName($_POST["id"]);
        $type = $_POST["type"];
        $storage = $_POST["storage"];
        $warehousetype = $_POST["warehousetype"];
        $uniqueid = $_POST["uniqueid"];
        $active = $_POST["active"];

        // Initialize the Warehouse object and set values
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

        // Check if the warehouse already exists
        $query_check = $Warehouse->checkInsert($Warehouse);
        $query_result = mysqli_query($con, $query_check);
        $numrows = mysqli_num_rows($query_result);

        if ($numrows != 0) {
            $row = mysqli_fetch_assoc($query_result);
            $uniqueid_check = $row["uniqueid"];
            if ($uniqueid != $uniqueid_check) {
                echo "Error: Warehouse ID already exists. ID: " . $id;
                exit();
            }
        }

        // Update the warehouse information
        $query = $Warehouse->update($Warehouse);
        mysqli_query($con, $query);
        mysqli_close($con);

        // Redirect after successful update
        echo "<script>window.location.href = '../MlspWarehouse.php';</script>";
    } else {
        // Incorrect password
        echo "Error: Incorrect password";
        return;
    }
} else {
    // Username does not exist
    echo "Error: Username does not exist";
    return;
}

?>
<?php require('Fullui.php'); ?>
