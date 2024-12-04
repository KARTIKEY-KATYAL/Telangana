<?php

require('../util/Connection.php');
require('../structures/FPS.php');
require('../util/SessionFunction.php');
require('../structures/Login.php');

if (!SessionCheck()) {
    return;
}
$district = $_SESSION["district_district"];

require('Header.php');

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

$FPS = new FPS;
$FPS->setUniqueid($_POST['uid']);

// Prepare the delete query
$query = $FPS->delete($FPS);

// Check if all districts should be deleted
if ($_POST['uid'] == "all") {
    $query = $FPS->deletealldistrict($FPS, $district);
}

mysqli_query($con, $query);
mysqli_close($con);

echo "<script>window.location.href = '../FPS.php';</script>";

?>
<?php require('Fullui.php'); ?>
