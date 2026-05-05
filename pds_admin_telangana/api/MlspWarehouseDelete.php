<?php

require('../util/Connection.php');
require('../structures/MlspWarehouse.php');
require('../util/SessionFunction.php');
require('../structures/Login.php');

if(!SessionCheck()){
    return;
}

require('Header.php');

// Initialize the Login object and set username and password
$person = new Login;
$person->setUsername($_POST["username"]);
$person->setPassword($_POST["password"]);

// Ensure that the session user matches the provided username
if($_SESSION['user'] != $person->getUsername()){
    echo "User is logged in with a different username and password";
    return;
}

// Query to fetch the user details based on the username
$query = "SELECT * FROM login WHERE username='".$person->getUsername()."'";
$result = mysqli_query($con, $query);
$row = mysqli_fetch_assoc($result);

// Verify the entered password against the hashed password in the database
if ($row) {
    if (password_verify($person->getPassword(), $row['password'])) {
        // Password is correct, proceed with warehouse deletion

        // Initialize Warehouse object and set unique ID
        $Warehouse = new MlspWarehouse;
        $Warehouse->setUniqueid($_POST['uid']);

        // Generate the delete query based on the UID provided
        $query = $Warehouse->delete($Warehouse);

        // If 'all' is passed as the UID, delete all warehouses
        if($_POST['uid'] == "all"){
            $query = $Warehouse->deleteall($Warehouse);
        }

        // Execute the delete query and close the database connection
        mysqli_query($con, $query);
        mysqli_close($con);

        // Redirect to the warehouse page after successful deletion
        echo "<script>window.location.href = '../MlspWarehouse.php';</script>";
    } else {
        // If password is incorrect
        echo "Error: Password is incorrect";
        return;
    }
} else {
    // If username is not found
    echo "Error: Username does not exist";
    return;
}

?>
<?php require('Fullui.php'); ?>
