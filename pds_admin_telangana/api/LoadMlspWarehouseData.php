<?php

@set_time_limit(0);
@ini_set('max_execution_time', '0');
@error_reporting(0);
@ini_set('display_errors', '0');

// Send JSON header before any output
header('Content-Type: application/json');

require('../util/Connection.php');
require('../util/SessionFunction.php');

if (!SessionCheck()) {
    echo json_encode(['status' => 'error', 'message' => 'Session expired. Please login again.']);
    exit();
}

$payload = json_encode([
    'month' => (string)(int)date('n'),
    'year'  => (string)(int)date('Y')
]);

$ch = curl_init();
curl_setopt_array($ch, [
    CURLOPT_URL            => 'https://smartpds.telangana.gov.in/Metadata/api/metadata/mlswidemetadatanew',
    CURLOPT_RETURNTRANSFER => true,
    CURLOPT_ENCODING       => '',
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
    CURLOPT_SSL_VERIFYPEER => false,
    CURLOPT_SSL_VERIFYHOST => false,
    CURLOPT_IPRESOLVE      => CURL_IPRESOLVE_V4,
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
    echo json_encode(['status' => 'error', 'message' => 'Invalid response from external API']);
    exit();
}

// Support both direct array and wrapped { data: [...] } formats
$whData = isset($apiResponse['data']) && is_array($apiResponse['data'])
    ? $apiResponse['data']
    : $apiResponse;

if (empty($whData)) {
    echo json_encode(['status' => 'error', 'message' => 'No MLSP Warehouse data found for the current period']);
    exit();
}

// TRUNCATE for fast fresh load
mysqli_query($con, "TRUNCATE TABLE mlsp_warehouse");

// Disable autocommit and use batch inserts for speed
mysqli_autocommit($con, false);

$inserted  = 0;
$errors    = 0;
$batchSize = 500;
$values    = [];

function first_non_empty_value(array $item, array $keys, $default = '') {
    foreach ($keys as $key) {
        if (!isset($item[$key])) {
            continue;
        }

        $value = $item[$key];
        if ($value === null) {
            continue;
        }

        if (is_string($value)) {
            $normalized = trim($value);
            if ($normalized === '' || strcasecmp($normalized, 'NA') === 0 || strcasecmp($normalized, 'N/A') === 0 || strcasecmp($normalized, 'NULL') === 0 || strcasecmp($normalized, 'NONE') === 0) {
                continue;
            }
        }

        return $value;
    }

    return $default;
}

function get_inventory_total(array $item, $default = 'NA') {
    if (empty($item['inventories']) || !is_array($item['inventories'])) {
        return $default;
    }

    $total = 0;
    $found = false;

    foreach ($item['inventories'] as $inventory) {
        if (!is_array($inventory)) {
            continue;
        }

        if (isset($inventory['inventory']) && is_numeric($inventory['inventory'])) {
            $total += (float)$inventory['inventory'];
            $found = true;
        }
    }

    return $found ? (string)$total : $default;
}

foreach ($whData as $item) {
    if (empty($item['id']) || empty($item['name']) || empty($item['district'])) {
        $errors++;
        continue;
    }

    $lat = $item['latitude']  ?? null;
    $lng = $item['longitude'] ?? null;
    if ($lat === null || $lat === '' || !is_numeric($lat) ||
        $lng === null || $lng === '' || !is_numeric($lng)) {
        $errors++;
        continue;
    }

    $id            = mysqli_real_escape_string($con, trim($item['id']));
    $name          = mysqli_real_escape_string($con, trim($item['name'] ?? ''));
    $district      = mysqli_real_escape_string($con, trim($item['district']));
    $warehousetype = mysqli_real_escape_string(
        $con,
        trim(first_non_empty_value($item, ['warehousetype', 'warehouse_type', 'type'], 'MLSP'))
    );
    $type          = mysqli_real_escape_string(
        $con,
        trim(first_non_empty_value($item, ['block', 'motorable', 'road_type', 'roadtype', 'warehouse_block'], 'Motorable'))
    );
    $latitude      = mysqli_real_escape_string($con, trim($lat));
    $longitude     = mysqli_real_escape_string($con, trim($lng));
    $storage       = mysqli_real_escape_string(
        $con,
        trim(first_non_empty_value($item, ['storage', 'storage_capacity', 'capacity', 'stored_quantity'], '0'))
    );
    $active        = (int)($item['active'] ?? 1);
    $uniqueid      = substr(uniqid('WH_'), 0, 15);

    $values[] = "('$district','$name','$id','$warehousetype','$type','$latitude','$longitude','$storage','$uniqueid','$active')";

    if (count($values) >= $batchSize) {
        $sql = "INSERT INTO mlsp_warehouse (district,name,id,warehousetype,type,latitude,longitude,storage,uniqueid,active) VALUES " . implode(',', $values);
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
    $sql = "INSERT INTO mlsp_warehouse (district,name,id,warehousetype,type,latitude,longitude,storage,uniqueid,active) VALUES " . implode(',', $values);
    if (mysqli_query($con, $sql)) {
        $inserted += count($values);
    } else {
        $errors += count($values);
    }
    mysqli_commit($con);
}

mysqli_autocommit($con, true);
mysqli_close($con);

echo json_encode([
    'status'   => 'success',
    'message'  => "Data loaded successfully. Inserted: $inserted, Errors: $errors",
    'inserted' => $inserted,
    'errors'   => $errors
]);
