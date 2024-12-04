<?php

require('../util/Connection.php');
require('../structures/Warehouse.php');
require('../util/SessionFunction.php');
require('../structures/Login.php');

if(!SessionCheck()){
    return;
}

require('Header.php');

// Sanitize and format the name
function formatName($name) {
    $name = htmlspecialchars($name, ENT_QUOTES, 'UTF-8');
    $name = preg_replace('/[^a-zA-Z0-9_ ]/', '', $name); // Only allow alphanumeric, spaces, and underscores
    $name = ucwords(strtolower($name)); // Convert to title case
    return trim($name);
}

// Validate latitude and longitude
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

// Check if a value is numeric
function isStringNumber($stringValue) {
    return is_numeric($stringValue);
}

$person = new Login;
$person->setUsername($_POST["username"]);
$person->setPassword($_POST["password"]);

// Ensure the session user matches the provided username
if($_SESSION['user'] != $person->getUsername()){
    echo "User is logged in with a different username and password";
    return;
}

// Query to retrieve the user details from the database based on the username
$query = "SELECT * FROM login WHERE username='".$person->getUsername()."'";
$result = mysqli_query($con, $query);
$row = mysqli_fetch_assoc($result);

// Verify the entered password against the hashed password in the database
if ($row) {
    if (password_verify($person->getPassword(), $row['password'])) {
        // Password is correct, proceed with warehouse insertion

        // Validate latitude and longitude
        if(!isValidCoordinate($_POST["latitude"], 'latitude') || !isValidCoordinate($_POST["longitude"], 'longitude')){
            echo "Error : Check Latitude and Longitude Value";
            exit();
        }

        // Validate storage value
        if(!isStringNumber($_POST["storage"])){
            echo "Error : Check Storage Value";
            exit();
        }

        // Sanitize and format warehouse inputs
        $district = formatName($_POST["district"]);
        $latitude = $_POST["latitude"];
        $longitude = $_POST["longitude"];
        $name = formatName($_POST["name"]);
        $id = formatName($_POST["id"]);
        $type = $_POST["type"];
        $storage = $_POST["storage"];
        $warehousetype = $_POST["warehousetype"];
        $uniqueid = uniqid("WH_", true);

        // Set the warehouse details
        $Warehouse = new Warehouse;
        $Warehouse->setUniqueid(substr($uniqueid, 0, 15));  // Limit the unique ID to 15 characters
        $Warehouse->setDistrict($district);
        $Warehouse->setLatitude($latitude);
        $Warehouse->setLongitude($longitude);
        $Warehouse->setName($name);
        $Warehouse->setId($id);
        $Warehouse->setType($type);
        $Warehouse->setStorage($storage);
        $Warehouse->setWarehousetype($warehousetype);
        $Warehouse->setActive("1");

        // Check if the warehouse already exists
        $query_insert_check = $Warehouse->checkInsert($Warehouse);
        $query_insert_result = mysqli_query($con, $query_insert_check);
        $numrows_insert = mysqli_num_rows($query_insert_result);

        // If no existing warehouse, insert the new warehouse
        if($numrows_insert == 0){
            $query = $Warehouse->insert($Warehouse);
            mysqli_query($con, $query);
            mysqli_close($con);
            echo "<script>window.location.href = '../Warehouse.php';</script>";
        } else {
            echo "Error : Warehouse ID already exists";
        }

    } else {
        // Password is incorrect
        echo "Error: Password is incorrect";
        return;
    }
} else {
    // Username not found
    echo "Error: Username does not exist";
    return;
}

?>
<?php require('Fullui.php'); ?>
