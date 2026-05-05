<?php

require('../util/Connection.php');
require('../structures/MlspWarehouse.php');
require('../util/SessionFunction.php');
require('../structures/Login.php');

if(!SessionCheck()){
	return;
}

$district = $_SESSION["district_district"];

require('Header.php');

// Create a new Login object
$person = new Login;
$person->setUsername($_POST["username"]);
$person->setPassword($_POST["password"]);

// Check if the session username matches the posted username
if ($_SESSION['district_user'] != $person->getUsername()) {
    echo "User is logged in with a different username and password";
    return;
}

// Fetch user data based on the username
$query = "SELECT * FROM login WHERE username='".$person->getUsername()."'";
$result = mysqli_query($con, $query);
$numrows = mysqli_num_rows($result);

if ($numrows == 0) {
    echo "Error: Username does not exist";
    return;
}

$row = mysqli_fetch_assoc($result);
$hashedPassword = $row['password'];

// Verify the hashed password
if (!password_verify($person->getPassword(), $hashedPassword)) {
    echo "Error: Password or Username is incorrect";
    return;
}

// Create a Warehouse object
$Warehouse = new MlspWarehouse;
$Warehouse->setUniqueid($_POST['uid']);

// Check if the "uid" field is "all" and delete all warehouses in the district
if ($_POST['uid'] == "all") {
    $query = $Warehouse->deletealldistrict($Warehouse, $district);
} else {
    $query = $Warehouse->delete($Warehouse);
}

// Execute the delete query
mysqli_query($con, $query);
mysqli_close($con);

// Redirect to the Warehouse page
echo "<script>window.location.href = '../MlspWarehouse.php';</script>";

?>
<?php require('Fullui.php'); ?>
