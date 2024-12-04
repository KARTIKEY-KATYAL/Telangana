<?php
require('../util/Connection.php');
require('../structures/Login.php');

require('Header.php');

$person = new Login;
$person->setUsername($_POST["username"]);
$person->setPassword($_POST["password"]);

// Fetch the user details based on the username
$query = "SELECT * FROM login WHERE username='" . $person->getUsername() . "'";
$result = mysqli_query($con, $query);
$numrows = mysqli_num_rows($result);

if ($numrows == 0) {
    echo "Error: Username not found";
} else {
    $row = mysqli_fetch_assoc($result);
    $hashedPassword = $row['password']; // Get the hashed password from the database

    // Verify the password using password_verify()
    if (!password_verify($person->getPassword(), $hashedPassword)) {
        echo "Error: Password is incorrect";
    } else {
        $count = 1 + $row['count'];

        if ($row["verified"] == 0) {
            echo "Error: Your account needs to be verified, please contact admin";
        } else {
            $uniqueId = uniqid();
            $authToken = md5($uniqueId);
            $currentLoginTime = date("Y-m-d H:i:s");

            // Update the login information
            $queryUpdate = "UPDATE login SET token='$authToken', lastlogin='$currentLoginTime', count='$count' WHERE username='" . $person->getUsername() . "'";
            mysqli_query($con, $queryUpdate);

            // Set session variables
            $_SESSION['district_user'] = $person->getUsername();
            $_SESSION['district_password'] = $person->getPassword(); // Consider storing this securely if needed
            $_SESSION['district_district'] = $row["role"];
            $_SESSION['district_token'] = $authToken;

            mysqli_close($con);
            echo "<script>window.location.href = '../Home.php';</script>";
        }
    }
}

?>
<?php require('Fullui.php'); ?>
