<?php

namespace Stanford\Apache2;

// gets most prior value in $array up to date-time as set by $dttm parameter
function mostPrior($dttm, $array) {
    // create new associative array of objects with date-time on or before $dttm
    $new_array = [];
    foreach ($array as $obj) {
        if (strtotime($obj["dttm"]) <= strtotime($dttm)) {
            $new_array[] = $obj;
        }
    }

    // get most recent value based on dttm, return -99 if none found i.e., empty new_array
    $latest_value = -99;
    if($new_array) {
        usort(
            $new_array,
            function($a, $b) {
                $dttm_1 = strtotime($a["dttm"]);
                $dttm_2 = strtotime($b["dttm"]);
                return $dttm_2 <=> $dttm_1;
            }
        );
        $latest_value = $new_array[0]["value"];
    }
    return $latest_value;
}

// calculates and returns A-a Gradient
function calculateAado2($pao2_val, $paco2_val, $fio2_val) {
    $atm_pressure = 760; // atmospheric pressure
    $h2o_pressure = 47; // water vapor pressure
    $rq = 0.8; // respiratory quotient, constant at 0.8 for all except those with the most extreme and unusual of diets
    return (($fio2_val / 100) * ($atm_pressure - $h2o_pressure) - ($paco2_val / $rq)) - $pao2_val;
}

// returns maximum calculated A-a Gradient
// returns -99 if no A-a Gradients could be calculated
function getMaxAado2($pao2_arr, $paco2_arr, $fio2_arr)
{
    $aado2_max = -99;
    $aado2_arr = [];
    foreach ($pao2_arr as $pao2) {
        // echo "pao2: ${pao2["value"]} dttm: ${pao2["dttm"]}\n";
        $paco2_val = mostPrior($pao2["dttm"], $paco2_arr);
        // echo "paco2: ${paco2_val}\n";
        $fio2_val = mostPrior($pao2["dttm"], $fio2_arr);
        // echo "fio2: ${fio2_val}\n";
        if ($paco2_val === -99 || $fio2_val === -99) {
            continue;
        } else {
            $aado2_val = calculateAado2($pao2["value"], $paco2_val, $fio2_val);
            $aado2_arr[] = $aado2_val;
            // echo "aado2: ${aado2_val}\n";
        }
    }
    if ($aado2_arr) {
        $aado2_max = max($aado2_arr);
    }
    return $aado2_max;
}

/*

// read in JSON files

$fio2_file = file_get_contents('/Users/jonaselr/Downloads/fio2-results.json', FILE_USE_INCLUDE_PATH);
$fio2_arr = json_decode($fio2_file, true);
$paco2_file = file_get_contents('/Users/jonaselr/Downloads/paco2-results.json', FILE_USE_INCLUDE_PATH);
$paco2_arr = json_decode($paco2_file, true);
$pao2_file = file_get_contents('/Users/jonaselr/Downloads/pao2-results.json', FILE_USE_INCLUDE_PATH);
$pao2_arr = json_decode($pao2_file, true);

echo "fio2:\n";
var_dump($fio2_arr);
echo "paco2:\n";
var_dump($paco2_arr);
echo "pao2:\n";
var_dump($pao2_arr);

echo getMaxAado2($pao2_arr, $paco2_arr, $fio2_arr);

*/
