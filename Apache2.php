<?php
namespace Stanford\Apache2;

require_once "emLoggerTrait.php";
// require_once "Apache2AaGradient.php";

class Apache2 extends \ExternalModules\AbstractExternalModule {

    use emLoggerTrait;

    public function __construct() {
		parent::__construct();
		// Other code to run when object is instantiated
	}


    /*
    // Triggered when a module gets enabled on Control Center.
	public function redcap_module_system_enable( $version ) {

	}

    // Triggered when a module gets enabled on a specific project.
	public function redcap_module_project_enable( $version, $project_id ) {

	}

    // Triggered after a module configuration is saved.
	public function redcap_module_save_configuration( $project_id ) {

	}
    */
}

/*
need to...
ICU-39
sync then data retrieval (query per patient? or per parameter? per parameter per patient, all parameters per patient, per parameter for all records, all parameters for all records
    make REDCap to STARR Link instance and make query via RedcapToStarrDataClass::retrieveData()
        $rtsl = \ExternalModules\ExternalModules::getModuleInstance('redcap_to_starr_link');
        // sync records via $rtsl->syncRecords($pid);
        $response = $rtsl->streamData($pid, $query_name, $arm, $fields);

ICU-40
parse csv of results into pao2 (see screenshot example)

ICU-41
use Apache2AaGradient functions to process data

ICU-42
save results from Apache2AaGradient functions to REDCap via http://localhost/redcap/redcap_v11.4.0/Plugins/index.php?REDCapMethod=saveData

ICU-37
configure EM Logger

consider...
	queries for incremental enrollment vs mass upload
	error-handling for end-user e.g., wrong MRN
