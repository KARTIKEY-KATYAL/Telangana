<?php

require('../util/Connection.php');
require('../structures/District.php');
require('../util/SessionFunction.php');
require('../structures/Login.php');

if(!SessionCheck()){
	return;
}

require('Header.php');


$person = new Login;
$person->setUsername($_POST["username"]);
$person->setPassword($_POST["password"]);

if($_SESSION['user']!=$person->getUsername()){
	echo "User is logged in with different username and password";
	return;
}

// Query the database to get the hashed password for the username
$query = "SELECT * FROM login WHERE username='".$person->getUsername()."'";
$result = mysqli_query($con, $query);
$row = mysqli_fetch_assoc($result);

// Check if the username exists and verify the password using password_verify
if ($row) {
    // Use password_verify to compare the entered password with the hashed password
    if (password_verify($person->getPassword(), $row['password'])) {
        // Password is correct, proceed with the delete operation
        $District = new District;
        $District->setId($_POST['uid']);

$query = $District->delete($District);

mysqli_query($con,$query);
mysqli_close($con);

echo "<script>window.location.href = '../District.php';</script>";

    } else {
        echo "Error: Password is incorrect";
        return;
    }
} else {
    echo "Error: Username does not exist";
    return;
}

?>
<?php require('Fullui.php');  ?>