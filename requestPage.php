<?php
namespace Stanford\Apache2DataFeed;
require_once "emLoggerTrait.php";

use REDCap;

global $module;

function checkFields() {
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
            return 0;
        }
    }
    return 1;
}

if($_SERVER["REQUEST_METHOD"] == "POST") {
    // instantiate REDCap to STARR Link
    $rtsl = \ExternalModules\ExternalModules::getModuleInstance('redcap_to_starr_link');

    // sync records via REDCap to STARR Link
    $rtsl->syncRecords($pid); // sync records

    // query and save parameters via REDCap to STARR Link, excluding A-a Gradient scores
    $flowlab_csv = $rtsl->streamData($pid, 'apache2_flowlabs', 1, array());
    $flowlab_results = $module->parseFlowLabCSV($flowlab_csv);
    $module->saveResults($pid, $flowlab_results);


    // process A-a Gradient scores
    $aao2_csv = $rtsl->streamData($pid, 'apache2_aao2', 1, array());
    $aao2_data = $module->parseAao2CSV($aao2_csv);
    $records = $module->getRecords($pid);
    $aao2_results = $module->getAao2Scores($records,
                                           $aao2_data['pao2'],
                                           $aao2_data['paco2'],
                                           $aao2_data['fio2']);
    $module->emDebug($aao2_results);
    $module->saveResults($pid, $aao2_results);

    exit("Data saved.");
}

require_once APP_PATH_DOCROOT . 'ProjectGeneral/header.php'; // maintain sidebar
?>

<!doctype html>
<html lang="en">
    <head>
        <meta charset="utf-8">
        <meta name="viewport" content="width=device-width, initial-scale=1">
        <script src="https://ajax.googleapis.com/ajax/libs/jquery/3.6.0/jquery.min.js" crossorigin="anonymous"></script>
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
        if (checkFields() === 1) { ?>
            <div>
                <p>Hit the 'Get Data' button below to run the automated process:</p>
                <button id="submit">Get Data</button>
                <div id="submit-message"></div>
            </div>
        <?php
        } else {
        ?>
            <div>
                <h1>ERROR!</h1>
                <p>Your project is missing necessary fields for this external module.</p>
                <p>Add the TDS-Research APACHE II Score Data Dictionary to your project's data dictionary.</p>
                <a href=<?php echo $module->getUrl("TDS-Research_ApacheII_Score_DataDictionary.csv", false, true); ?> download="TDS-Research_ApacheII_Score_DataDictionary.csv">
                    Download the TDS-Research APACHE II Score Data Dictionary
                </a>
            </div>
        <?php
        }
        ?>

        <script>
            $("#submit").click(function() {
                $("#submit-message").html("Running the data feed...");
                $.post("", function(data) {
                    console.log(data);
                    $("#submit-message").html(data);
                });
            });
        </script>
    </body>
</html>
