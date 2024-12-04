<?php
require('../util/Connection.php');
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

// Query the database to get the stored hash for the username
$query = "SELECT * FROM login WHERE username='".$person->getUsername()."'";
$result = mysqli_query($con, $query);
$row = mysqli_fetch_assoc($result);

if ($row) {
    // Use password_verify to check if the entered password matches the hashed password in the database
    if (password_verify($person->getPassword(), $row['password'])) {
        // Password is correct
        $uid = $_POST["uid"];
        $query = "DELETE FROM login WHERE uid='$uid'";
        mysqli_query($con, $query);
        mysqli_close($con);
        echo "<script>window.location.href = '../Userdata.php';</script>";
    } else {
        // Password is incorrect
        echo "Error : Password is incorrect";
        return;
    }
} else {
    // Username not found
    echo "Error : Username does not exist";
    return;
}
?>
<?php require('Fullui.php');  ?>