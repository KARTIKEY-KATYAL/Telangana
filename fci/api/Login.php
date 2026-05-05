<?php

require('../util/Connection.php');
require('../util/Security.php');
require('../structures/Login.php');
require ('../util/Encryption.php');
session_start();
$person = new Login;
$person->setUsername($_POST["username"]);
$person->setPassword($_POST["password"]);
$nonceValue = 'nonce_value';

if (empty($person->getUsername()) || empty($person->getPassword())){
    echo "Username and password are required.";
    exit;
}



$Encryption = new Encryption();
$person->setPassword($Encryption->decrypt($_POST["password"], $nonceValue));


$query = "SELECT * FROM login WHERE username='".$person->getUsername()."'";
$result = mysqli_query($con,$query);
$numrows = mysqli_num_rows($result);

if($numrows == 0){
	echo "Username and password are incorrect.";
}
else if($numrows > 0){
	$row = mysqli_fetch_assoc($result);
	$dbHashedPassword = $row['password'];
	if(password_verify($person->getPassword(), $dbHashedPassword)){
        if($row['role']=="admin"){
            $count = 1 + $row['count'];
            $uniqueId = uniqid();
            $authToken = md5($uniqueId);
            $currentLoginTime = date("Y-m-d H:i:s");
            $queryUpdate = "UPDATE login SET token='$authToken',lastlogin='$currentLoginTime',count='$count' WHERE username='".$person->getUsername()."'";
            mysqli_query($con,$queryUpdate);
            
            $_SESSION['user'] = $person->getUsername();
            $_SESSION['token'] = $authToken;
            
            mysqli_close($con);
            echo "<script>window.location.href = '../Mill.php';</script>";
        }
	}
	else{
		echo "Username and password incorrect.";
	}
}

?>
