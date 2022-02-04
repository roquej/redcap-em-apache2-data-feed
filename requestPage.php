<?php
namespace Stanford\Apache2DataFeed;
require_once "emLoggerTrait.php";
use REDCap;

global $module;

/**
 * checks if REDCap project has the necessary field names for the data feed to save to
 * @return bool
 */
function hasFields() {
    $apache2_fields = array('tdsr_apache2_icuadmit_dttm', 'tdsr_apache2_age', 'tdsr_apache2_age_score',
        'tdsr_apache2_immuno_score', 'tdsr_apache2_temp_min', 'tdsr_apache2_temp_max', 'tdsr_apache2_temp_score',
        'tdsr_apache2_map_min', 'tdsr_apache2_map_max', 'tdsr_apache2_map_score',
        'tdsr_apache2_hr_min', 'tdsr_apache2_hr_max', 'tdsr_apache2_hr_score',
        'tdsr_apache2_rr_min', 'tdsr_apache2_rr_max', 'tdsr_apache2_rr_score',
        'tdsr_apache2_gcs_min', 'tdsr_apache2_gcs_max', 'tdsr_apache2_gcs_score',
        'tdsr_apache2_pha_min', 'tdsr_apache2_pha_max', 'tdsr_apache2_pha_score',
        'tdsr_apache2_fio2_max', 'tdsr_apache2_pao2_min', 'tdsr_apache2_pao2_max', 'tdsr_apache2_aao2_min', 'tdsr_apache2_aao2_max', 'tdsr_apache2_o2_score',
        'tdsr_apache2_na_min', 'tdsr_apache2_na_max', 'tdsr_apache2_na_score', 'tdsr_apache2_k_min', 'tdsr_apache2_k_max', 'tdsr_apache2_k_score',
        'tdsr_apache2_arf', 'tdsr_apache2_cr_min', 'tdsr_apache2_cr_max', 'tdsr_apache2_cr_score',
        'tdsr_apache2_hct_min', 'tdsr_apache2_hct_max', 'tdsr_apache2_hct_score', 'tdsr_apache2_wbc_min', 'tdsr_apache2_wbc_max', 'tdsr_apache2_wbc_score',
        'tdsr_apache2_score');
    $fields = REDCAP::getFieldNames();
    foreach($apache2_fields as $apache2_field) {
        if(!in_array($apache2_field, $fields)) {
            return false;
        }
    }
    return true;
}

// initiates the data feed process
// triggered when user clicks button to request data
if($_SERVER["REQUEST_METHOD"] == "POST") {
    $module->emDebug("Data feed process started");

    $success_exit = "Data was successfully retrieved and saved to your REDCap project.";
    $failure_exit = "ERROR- Data feed failed to retrieve and save data.";
    $no_new_data = "The data feed was successfully completed, but there was either no retrievable data to save or no changes to save in REDCap." ;

    // instantiate REDCap to STARR Link
    $rtsl = \ExternalModules\ExternalModules::getModuleInstance('redcap_to_starr_link');

    // sync records via REDCap to STARR Link
    if($rtsl->syncRecords($pid) === false) {
        $module->emError("rtsl->syncRecords() failed to sync records on Project " . $pid);
        exit($failure_exit);
    }

    // query and save parameters via REDCap to STARR Link, excluding A-a Gradient scores
    $flowlab_csv = $rtsl->streamData($pid, 'apache2_flowlabs', 1, array());
    $module->emDebug("Results of RtSL query 'apache2_flowlabs' via streamData(): " . $flowlab_csv);
    if($flowlab_csv === false) {
        $module->emError("rtsl->streamData() failed to retrieve data for query 'apache2_flowlabs'");
        exit($failure_exit);
    }
    $flowlab_results = $module->parseFlowLabCSV($flowlab_csv);
    if($flowlab_results === false) {
        $module->emError("removeHeaders() in parseFlowLabCSV() failed to remove headers from data streamed");
        exit($failure_exit);
    }
    $module->emDebug("Attempting to save flowsheet/lab data to REDCap.");
    $flowlab_save_response = REDCap::saveData('json', json_encode($flowlab_results), 'overwrite');
    $module->emDebug($flowlab_save_response);
    if(!empty($flowlab_save_response["errors"])) {
        $module->emError("Failed to save flowsheet/lab data to REDCap.");
        exit($failure_exit);
    }

    // process A-a Gradient scores
    $aao2_csv = $rtsl->streamData($pid, 'apache2_aao2', 1, array());
    if($aao2_csv === false) {
        $module->emError("rtsl->streamData() failed to retrieve data for query 'apache2_aao2'");
        exit($failure_exit);
    }
    $module->emDebug("Results of RtSL query 'apache2_aao2' via streamData(): " . $aao2_csv);
    $aao2_data = $module->parseAao2CSV($aao2_csv);
    if($aao2_data === false) {
        $module->emError("removeHeaders() in parseAao2CSV() failed to remove headers from data streamed");
        exit($failure_exit);
    }
    // $records = $module->getRecords();
    $module->emDebug("Attempting to retrieve project record IDs.");
    $records = REDCap::getData(array('return_format' => 'json', 'fields' => array('record_id')));
    $module->emDebug("List of records retrieved: " . $records);
    if(is_null($records)) {
        $module->emError("Failed to retrieve list of projects record IDs for A-a Gradient calculations");
        exit($failure_exit);
    }
    $aao2_results = $module->getAao2Scores($records,
                                           $aao2_data['pao2'],
                                           $aao2_data['paco2'],
                                           $aao2_data['fio2']);
    $module->emDebug("Attempting to save A-a gradient data to REDCap.");
    $aao2_save_response = REDCap::saveData('json', json_encode($aao2_results), 'overwrite');
    $module->emDebug($aao2_save_response);
    if(!empty($aao2_save_response["errors"])) {
        $module->emError("Failed to save A-a gradient data to REDCap.");
        exit($failure_exit);
    }
    if($flowlab_save_response["item_count"] === 0 && $aao2_save_response["item_count"] === 0) {
        exit($no_new_data);
    }
    exit($success_exit);
}

require_once APP_PATH_DOCROOT . 'ProjectGeneral/header.php'; // maintain sidebar
?>

<!doctype html>
<html lang="en">
    <head>
        <meta charset="utf-8">
        <meta name="viewport" content="width=device-width, initial-scale=1">
        <title>APACHE II Data Feed EM</title>
    </head>
    <body>
        <h1>APACHE II Data Feed</h1>
        <p>This external module enables you to query STARR for APACHE II parameters and save results directly to your project.
            For each record, the date-time of ICU admission is extracted from STARR's ADT data based on their MRN and enrollment date.
            If a record's enrollment date doesn't fall within a hospital admission that includes an ICU admission, then no data is obtained for that record.</p>
        <p>IMPORTANT: This external module does not determine the following parameters, which must currently be extracted manually:</p>
        <ul>
            <li>History of Severe Organ Insufficiency or Immunocompromised</li>
            <li>Acute Renal Failure</li>
        </ul>
        <?php
        if (hasFields() === true) { ?>
            <div>
                <p>Hit the 'Get Data' button below to run the data feed:</p>
                <button id="apache2-submit">Get Data</button>
                <hr>
                <div id="apache2-submit-message"></div>
                <small><div id="apache2-submit-timestamp"></div></small>
            </div>
        <?php
        } else {
        ?>
            <div>
                <h1>ERROR!</h1>
                <p>Your project is missing necessary fields for this external module.</p>
                <p>
                    You will need the TDS-Research APACHE II Score Data Dictionary to your project's data dictionary.
                    A simple method is to use the link below to download a data collection instrument containing the necessary fields.
                    Then, you can upload the instrument ZIP file to your project via its Online Designer.
                    Feel free to rename the uploaded instrument's name to 'TDSR APACHE II Score'.
                    Furthermore, ensure that the fields in the provided data collection instrument are not renamed within your project.
                    Otherwise, the APACHE II Data Feed may not properly save data as intended.
                </p>
                <a href=<?php echo $module->getUrl("TDSR-Research_ApacheII_DataCollectionInstrument.zip", false, true); ?> download="TDSR-Research_ApacheII_DataCollectionInstrument.zip">
                    Download the TDS-Research APACHE II Score Data Collection Instrument
                </a>
                <p>Training on how to work with Data Dictionaries <a href="https://redcap.stanford.edu/redcap_v12.0.4/Home/index.php?action=training">is available here </a> in the "Building a project" section.</p>
            </div>
        <?php
        }
        ?>

        <script>
            $("#apache2-submit").click(function() {
                $("#apache2-submit").prop("disabled", true);
                $("#apache2-submit-message").html("Running the data feed...");
                $("#apache2-submit-timestamp").html(new Date($.now()));

                $.post('<?php echo $module->getUrl("requestPage.php", false, true); ?>', {}, function(data) {
                    $("#apache2-submit").prop("disabled", false);
                    console.log(data);
                    $("#apache2-submit-message").html(data);
                    $("#apache2-submit-timestamp").html(new Date($.now()));
                });
            });
        </script>
    </body>
</html>
