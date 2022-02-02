<?php
namespace Stanford\Apache2DataFeed;
require_once "emLoggerTrait.php";
use REDCap;

/**
 * Class Apache2DataFeed
 * @package Stanford\Apache2DataFeed
 *
 * This module utilizes the REDCap to STARR Link API to pull APACHE II parameters to STARR and save them to REDCap projects.
 *
 */
class Apache2DataFeed extends \ExternalModules\AbstractExternalModule {
    use emLoggerTrait;

    public function __construct() {
		parent::__construct();
    }

    /**
     * helper function for removing headers from data retrieved via REDCap to STARR Link
     * called by parseFlowLabCSV()
     * @param $csv_to_parse
     * @return array|false
     */
    public function removeHeaders($csv_to_parse) {
        $results_arr = str_getcsv($csv_to_parse, PHP_EOL);
        // iterate over $results_arr until all header lines are removed
        // stops when count($results_arr) > 1 in case a line starting with "redcap_record_id" is never matched
        // to prevent infinite loop
        while(substr($results_arr[0], 0, 16) !== "redcap_record_id" && count($results_arr) > 1) {
            array_shift($results_arr);
        }
        // if while loop exited due to no header match
        if(count($results_arr) === 1 && substr($results_arr[0], 0, 16) !== "redcap_record_id") {
            return false;
        }
        array_shift($results_arr);
        $this->emDebug($results_arr);
        return $results_arr;
    }

    /**
     * parses data retrieved via REDCap to STARR Link query 'apache2_flowlabs'
     * @param $flowlab_csv
     * @return array|false
     */
    public function parseFlowLabCSV($flowlab_csv) {
        // parse CSV into array, line by line
        $flowlab_lines = $this->removeHeaders($flowlab_csv);
        if($flowlab_lines === false) {
            return false;
        }

        $flowlab_results = array();
        foreach($flowlab_lines as $line) {
            $result = str_getcsv($line, ',');
            $flowlab_results[] = array(
                "record_id" => $result[0],
                "tdsr_apache2_age" => $result[1],
                "tdsr_apache2_icuadmit_dttm" => $result[2],
                "tdsr_apache2_temp_min" => $result[3],
                "tdsr_apache2_temp_max" => $result[4],
                "tdsr_apache2_map_min" => $result[5],
                "tdsr_apache2_map_max" => $result[6],
                "tdsr_apache2_hr_min" => $result[7],
                "tdsr_apache2_hr_max" => $result[8],
                "tdsr_apache2_rr_min" => $result[9],
                "tdsr_apache2_rr_max" => $result[10],
                "tdsr_apache2_gcs_min" => $result[11],
                "tdsr_apache2_gcs_max" => $result[12],
                "tdsr_apache2_fio2_max" => $result[13],
                "tdsr_apache2_na_min" => $result[14],
                "tdsr_apache2_na_max" => $result[15],
                "tdsr_apache2_k_min" => $result[16],
                "tdsr_apache2_k_max" => $result[17],
                "tdsr_apache2_cr_min" => $result[18],
                "tdsr_apache2_cr_max" => $result[19],
                "tdsr_apache2_hct_min" => $result[20],
                "tdsr_apache2_hct_max" => $result[21],
                "tdsr_apache2_pha_min" => $result[22],
                "tdsr_apache2_pha_max" => $result[23],
                "tdsr_apache2_pao2_min" => $result[24],
                "tdsr_apache2_pao2_max" => $result[25],
                "tdsr_apache2_wbc_min" => $result[26],
                "tdsr_apache2_wbc_max" => $result[27]
            );
        }
        $this->emDebug($flowlab_results);
        return $flowlab_results;
    }

    /**
     * parses data retrieved via REDCap to STARR Link query 'apache2_aao2'
     * uses helper function removeHeaders();
     * @param $aao2_csv
     * @return array[
     *          'fio2' => array[
     *                       'id',
     *                       'dttm',
     *                       'value']
     *          'pao2' => array[
     *                       'id',
     *                       'dttm',
     *                       'value']
     *          'paco2' => array[
     *                       'id',
     *                       'dttm',
     *                       'value']
     *              ]|false
     */
    public function parseAao2CSV($aao2_csv)
    {
        $aao2_lines = $this->removeHeaders($aao2_csv);
        if($aao2_lines === false) {
            return false;
        }

        $fio2 = [];
        $pao2 = [];
        $paco2 = [];

        foreach($aao2_lines as $line) {
            $result = str_getcsv($line, ',');
            $type = $result[1];
            $recording = array(
                               "id"    => $result[0],
                               "dttm"  => $result[2],
                               "value" => $result[3]
                              );
            switch ($type) {
                case 'FiO2':
                    $fio2[] = $recording;
                    break;
                case 'PaO2':
                    $pao2[] = $recording;
                    break;
                case 'PaCO2':
                    $paco2[] = $recording;
                    break;
                default:
                    $this->emError($type . " did not match FiO2, PaO2, or PaCO2");
                    break;
            }
        }

        $aado2_data = [];
        $aado2_data['fio2']  = $fio2;
        $aado2_data['pao2']  = $pao2;
        $aado2_data['paco2'] = $paco2;
        $aado2_data = array(
                            'fio2'  => $fio2,
                            'pao2'  => $pao2,
                            'paco2' => $paco2
                           );
        $this->emDebug($aado2_data);
        return $aado2_data;
    }

    /**
     * Returns most prior value of PaCO2 up to the date-time as set by $pao2_dttm
     * Returns '-99' if there's no possible PaCO2 value
     * @param $pao2_dttm
     * @param $paco2_arr
     * @return int|mixed
     */
    public function mostPriorPaco2($pao2_dttm, $paco2_arr) {
        // create new associative array of objects with date-time on or before $pao2_dttm
        $new_paco2_arr = array_filter($paco2_arr, function ($key) use ($pao2_dttm) {
            return (strtotime($key['dttm']) <= strtotime($pao2_dttm));
        });

        // get most recent value based on $pao2_dttm, return -99 if none found i.e., empty $new_paco2_arr
        $latest_paco2 = -99;
        if(count($new_paco2_arr) > 0) {
            usort(
                $new_paco2_arr,
                function($a, $b) {
                    $dttm_1 = strtotime($a['dttm']);
                    $dttm_2 = strtotime($b['dttm']);
                    return $dttm_2 <=> $dttm_1;
                }
            );
            $latest_paco2 = $new_paco2_arr[0]['value'];
        }
        return $latest_paco2;
    }

    /**
     * Returns most prior value of FiO2 up to, but NOT at the same date-time, the date-time as set by $pao2_dttm
     * Returns '-99' if there's no possible FiO2 value
     * @param $pao2_dttm
     * @param $fio2_arr
     * @return int|mixed
     */
    public function mostPriorFio2($pao2_dttm, $fio2_arr) {
        // create new associative array of objects with date-time before $pao2_dttm
        $new_fio2_arr = array_filter($fio2_arr, function ($key) use ($pao2_dttm) {
            return (strtotime($key['dttm']) < strtotime($pao2_dttm));
        });

        // get most recent FiO2 value based on $pao2_dttm, return -99 if none found i.e., empty $new_fio2_arr
        $latest_fio2 = -99;
        if(count($new_fio2_arr) > 0) {
            usort(
                $new_fio2_arr,
                function($a, $b) {
                    $dttm_1 = strtotime($a['dttm']);
                    $dttm_2 = strtotime($b['dttm']);
                    return $dttm_2 <=> $dttm_1;
                }
            );
            $latest_fio2 = $new_fio2_arr[0]['value'];
        }
        return $latest_fio2;
    }

    /**
     * Calculates and returns an A-a Gradient score
     * @param $pao2_val
     * @param $paco2_val
     * @param $fio2_val
     * @return float|int
     */
    public function calculateAao2($pao2_val, $paco2_val, $fio2_val) {
        $atm_pressure = 760; // atmospheric pressure
        $h2o_pressure = 47; // water vapor pressure
        $rq = 0.8; // respiratory quotient, constant at 0.8 for all except those with the most extreme and unusual of diets
        return (floatval($fio2_val) * ($atm_pressure - $h2o_pressure) - (floatval($paco2_val) / $rq)) - floatval($pao2_val);
    }

    /**
     * Calculates all possible A-a Gadient scores
     * Returns minimum and maximum scores for each record
     * Returns '-99' as scores for records without any possible A-a Gradient scores
     * @param $records
     * @param $pao2_arr
     * @param $paco2_arr
     * @param $fio2_arr
     * @return array[]
     */
    public function getAao2Scores($records, $pao2_arr, $paco2_arr, $fio2_arr) {
        $aao2_minmax = [];
        $records_arr = json_decode($records);
        foreach($records_arr as $record) {
            $record_id = $record->record_id;
            $aao2_max = -99;
            $aao2_min = -99;
            $aao2_arr_record = [];
            $fio2_arr_record = array_filter($fio2_arr, function ($key) use ($record_id) {
                return ($key['id'] === $record_id);
            });
            $pao2_arr_record = array_filter($pao2_arr, function ($key) use ($record_id) {
                return ($key['id'] === $record_id);
            });
            $paco2_arr_record = array_filter($paco2_arr, function ($key) use ($record_id) {
                return ($key['id'] === $record_id);
            });
            foreach($pao2_arr_record as $pao2) {
                $paco2_val = $this->mostPriorPaco2($pao2['dttm'], $paco2_arr_record);
                $fio2_val = $this->mostPriorFio2($pao2['dttm'], $fio2_arr_record);
                if($paco2_val === -99 || $fio2_val === -99) {
                    continue;
                } else {
                    $pao2_val = $pao2['value'];
                    $aao2_val = $this->calculateAao2($pao2_val, $paco2_val, $fio2_val);
                    $aao2_arr_record[] = $aao2_val;
                }
            }
            if (count($aao2_arr_record) > 0) {
                $aao2_max = max($aao2_arr_record);
                $aao2_min = min($aao2_arr_record);
            }

            $aao2_minmax[] = array(
                'record_id' => strval($record->record_id),
                'tdsr_apache2_aao2_min' => strval($aao2_min),
                'tdsr_apache2_aao2_max' => strval($aao2_max)
            );
        }
        $this->emDebug($aao2_minmax);
        return $aao2_minmax;
    }
}
