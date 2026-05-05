<?php

session_start();
session_regenerate_id(true);
$_SESSION['name'] = null;
$_SESSION['user'] = null;
header("Location:../Login.html");

?>