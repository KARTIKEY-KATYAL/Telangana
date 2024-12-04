<?php

require('../util/Connection.php');
require('../structures/FPS.php');
require('../util/SessionFunction.php');
require('../structures/Login.php');

// Check if session is valid
if(!SessionCheck()){
    return;
}

require('Header.php');

// Create Login object and set credentials
$person = new Login;
$person->setUsername($_POST["username"]);
$person->setPassword($_POST["password"]);

// Ensure the current session user matches the one trying to perform the action
if($_SESSION['user'] != $person->getUsername()){
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

if($numrows == 0){
    echo "Error: Password or Username is incorrect.";
    return;
}

// Fetch user data from result
$row = mysqli_fetch_assoc($result);
$hashedPassword = $row['password'];

// Verify the provided password against the hashed password in the database
if(!password_verify($person->getPassword(), $hashedPassword)){
    echo "Error: Password or Username is incorrect.";
    return;
}

// Create FPS object and set the unique ID (with input sanitization)
$FPS = new FPS;
$uniqueId = mysqli_real_escape_string($con, $_POST['uid']);
$FPS->setUniqueid($uniqueId);

// Prepare query based on the unique ID
if($uniqueId == "all"){
    $query = $FPS->deleteall($FPS);
} else {
    $query = $FPS->delete($FPS);
}

// Execute the query
mysqli_query($con, $query);

// Close the database connection
mysqli_close($con);

// Redirect to FPS.php
echo "<script>window.location.href = '../FPS.php';</script>";

?>

<?php require('Fullui.php'); ?>
