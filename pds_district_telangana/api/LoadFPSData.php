<?php

@set_time_limit(0);
@ini_set('max_execution_time', '0');
@ini_set('memory_limit', '-1');
@error_reporting(0);
@ini_set('display_errors', '0');

ob_start(); // Buffer any stray output (BOM, newlines from included files)
header('Content-Type: application/json');

require('../util/Connection.php');
require('../util/SessionFunction.php');

if (!SessionCheck()) {
    ob_end_clean();
    echo json_encode(['status' => 'error', 'message' => 'Session expired. Please login again.']);
    exit();
}

$userDistrict = $_SESSION['district_district'];

$payload = json_encode([
    'month' => (string)(int)date('n'),
    'year'  => (string)(int)date('Y')
]);

$ch = curl_init();
curl_setopt_array($ch, [
    CURLOPT_URL            => 'https://smartpds.telangana.gov.in/Metadata/api/metadata/shopwidemetadatanew',
    CURLOPT_RETURNTRANSFER => true,
    CURLOPT_ENCODING       => '',           // Accept gzip/deflate compression
    CURLOPT_MAXREDIRS      => 10,
    CURLOPT_TIMEOUT        => 0,
    CURLOPT_CONNECTTIMEOUT => 0,
    CURLOPT_FOLLOWLOCATION => true,
    CURLOPT_HTTP_VERSION   => CURL_HTTP_VERSION_1_1,
    CURLOPT_CUSTOMREQUEST  => 'POST',
    CURLOPT_POSTFIELDS     => $payload,
    CURLOPT_HTTPHEADER     => [
        'Content-Type: application/json',
        'Accept: application/json'
    ],
    CURLOPT_SSL_VERIFYPEER => false,        // Skip SSL verify for speed
    CURLOPT_SSL_VERIFYHOST => false,
    CURLOPT_IPRESOLVE      => CURL_IPRESOLVE_V4,  // Force IPv4, skip IPv6 lookup
    CURLOPT_PROXY          => ''
]);

$response  = curl_exec($ch);
$httpCode  = curl_getinfo($ch, CURLINFO_HTTP_CODE);
$curlError = curl_error($ch);
curl_close($ch);

if ($response === false || $httpCode !== 200) {
    echo json_encode([
        'status'  => 'error',
        'message' => 'Failed to fetch data from external API. HTTP ' . $httpCode . '. ' . $curlError
    ]);
    exit();
}

$apiResponse = json_decode($response, true);
if (!$apiResponse || !is_array($apiResponse)) {
    ob_end_clean();
    echo json_encode(['status' => 'error', 'message' => 'Invalid response from external API']);
    exit();
}

// Support both direct array and wrapped { data: [...] } formats
$fpsData = isset($apiResponse['data']) && is_array($apiResponse['data'])
    ? $apiResponse['data']
    : $apiResponse;

if (empty($fpsData)) {
    ob_end_clean();
    echo json_encode(['status' => 'error', 'message' => 'No FPS data found for the current period']);
    exit();
}

// DELETE for the specific district
mysqli_query($con, "DELETE FROM fps WHERE district='$userDistrict'");

// Disable autocommit and use batch inserts for maximum speed
mysqli_autocommit($con, false);

$inserted  = 0;
$errors    = 0;
$batchSize = 500;
$values    = [];

foreach ($fpsData as $item) {
    if (empty($item['id']) || empty($item['name']) || empty($item['district'])) {
        continue;
    }

    if (strcasecmp(trim($item['district']), $userDistrict) !== 0) {
        continue;
    }

    $lat = $item['latitude']  ?? null;
    $lng = $item['longitude'] ?? null;
    if ($lat === null || $lat === '' || !is_numeric($lat) ||
        $lng === null || $lng === '' || !is_numeric($lng)) {
        $errors++;
        continue;
    }

    // Extract FRice demand specifically; fall back to summing all demands
    $demand = 0.0;
    if (!empty($item['demands']) && is_array($item['demands'])) {
        foreach ($item['demands'] as $d) {
            if (($d['commodity'] ?? '') === 'FRice') {
                $demand = (float)($d['demand'] ?? 0);
                break;
            }
        }
        if ($demand == 0.0) {
            foreach ($item['demands'] as $d) {
                $demand += (float)($d['demand'] ?? 0);
            }
        }
    } else {
        $demand = (float)($item['demand'] ?? 0);
    }

    $rawName  = trim($item['name'] ?? '');
    $name     = ($rawName === '' || $rawName === 'NA') ? 'FPS ' . $item['id'] : $rawName;

    $id        = mysqli_real_escape_string($con, trim($item['id']));
    $name      = mysqli_real_escape_string($con, $name);
    $district  = mysqli_real_escape_string($con, trim($item['district']));
    $type      = mysqli_real_escape_string($con, trim($item['type'] ?? 'Normal FPS'));
    $latitude  = mysqli_real_escape_string($con, trim($lat));
    $longitude = mysqli_real_escape_string($con, trim($lng));
    $demand    = round($demand, 2);
    $active    = (int)($item['active'] ?? 1);
    $uniqueid  = substr(uniqid('FPS_'), 0, 15);

    $values[] = "('$district','$name','$id','$type','$latitude','$longitude','$demand','$uniqueid','$active')";

    // Execute batch when limit reached
    if (count($values) >= $batchSize) {
        $sql = "INSERT INTO fps (district,name,id,type,latitude,longitude,demand,uniqueid,active) VALUES " . implode(',', $values);
        if (mysqli_query($con, $sql)) {
            $inserted += count($values);
        } else {
            $errors += count($values);
        }
        $values = [];
        mysqli_commit($con);
    }
}

// Insert remaining records
if (!empty($values)) {
    $sql = "INSERT INTO fps (district,name,id,type,latitude,longitude,demand,uniqueid,active) VALUES " . implode(',', $values);
    if (mysqli_query($con, $sql)) {
        $inserted += count($values);
    } else {
        $errors += count($values);
    }
    mysqli_commit($con);
}

mysqli_autocommit($con, true);
mysqli_close($con);

ob_end_clean();
echo json_encode([
    'status'   => 'success',
    'message'  => "Data loaded successfully. Inserted: $inserted, Errors: $errors",
    'inserted' => $inserted,
    'errors'   => $errors
]);
