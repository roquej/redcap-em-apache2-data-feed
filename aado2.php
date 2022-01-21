<?php
namespace Stanford\Apache2;
require_once "emLoggerTrait.php";
require_once APP_PATH_DOCROOT . 'ProjectGeneral/header.php'; // maintain sidebar

global $module;

if (isset($_POST['get_data'])) {
    // $module->emDebug("request submitted");

    // instantiate REDCap to STARR Link
    $rtsl = \ExternalModules\ExternalModules::getModuleInstance('redcap_to_starr_link');

    // sync records via REDCap to STARR Link
    $rtsl->syncRecords($pid); // sync records

    // query and save parameters via REDCap to STARR Link, excluding A-a Gradient scores
    $flowlab_csv = $rtsl->streamData($pid, 'apache2_flowlabs', 1, array());
    // $module->emDebug($flowlab_csv);
    $flowlab_results = $module->parseFlowLabCSV($flowlab_csv);
    $module->saveResults($pid, $flowlab_results);
    // $module->emDebug($flowlab_data);

    // process A-a Gradient scores
    $aao2_csv = $rtsl->streamData($pid, 'apache2_aao2', 1, array());
    // echo "Calculating A-a gradient scores... ";
    $aao2_data = $module->parseAao2CSV($aao2_csv);
    $records = $module->getRecords($pid);
    // $module->emDebug($records);
    $aao2_results = array_chunk($module->getAao2Scores($records,
                                                       $aao2_data['pao2'],
                                                       $aao2_data['paco2'],
                                                       $aao2_data['fio2']),
                                10);
    $module->emDebug($aao2_results);
    foreach($aao2_results as $chunk_results) {
        $module->saveResults($pid, $chunk_results);
    }
    // echo "A-a gradient calculations completed and saved.";

    $module->emDebug("FINISHED");
}

?>

<!doctype html>
<html lang = "en">
    <head>
    </head>
    <body>
        <p>Query and process data for APACHE II scoring:</p>
        <form action="" method="POST">
            <input type="submit" name="get_data">
        </form>
    </body>
</html>
