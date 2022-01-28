# Apache II Data Feed External Module

## Introduction
A REDCap External Module to extract [APACHE II](https://pubmed.ncbi.nlm.nih.gov/3928249/) scoring parameters based on EMR data.
When installed in a REDCap project, this EM provides a plugin page where users can navigate to and submit a request to pull APACHE II data for their cohort.
Clinical researchers can use this EM with a new or pre-existing IRB-approved REDCap project.

## How it works
Currently, this EM uses the REDCap to STARR Link EM to pull data based on MRN and enrollment date as entered in REDCap.
Most data for parameters require no post-processing after retrieval from STARR.
To calculate A-a gradient scores as part of the APACHE II, all FiO2 recordings, PaO2 labs, and PaCO2 labs are retrieved for each patient in their respective 24-hour window of APACHE II.
All possible A-a gradient scores are then calculated and the minimum and maximum scores are saved back to the REDCap project.

This EM relies on saving data to specific fields with a `tdsr_apache2` prefix (e.g., maximum WBC's field is `tdsr_apache2_wbc_max`).
These fields are provided in a data dictionary specifically for this EM.
Note that the EM itself doesn't actually calculate the scores.
Calculated fields provided in the data dictionary use the data fed by this EM to determine the subscores, which are then summed to the total APACHE II score.

Note: the enrollment date for patients should fall within an in-hospital admission containing an ICU admission.
Also, this EM currently only works with REDCap projects with a non-longitudinal, single arm design.

## For researchers (end users)
### IRB requirements
A data use agreement (attestation?) should be submitted for each project that uses this EM.

### User document
Researchers can refer to this user document for guidance on this APACHE II Data Feed EM, hosted on Google Drive:

[APACHE II Data Feed EM Installation/Configuration Instructions for Researchers]()

## Dependencies
### REDCap to STARR Link EM and queries to store
REDCap to STARR Link and its dependencies must be installed and configured to the project.
Refer to [DOCUMENT HERE]() for general guidance.

System-level REDCap to STARR Link specifics:
- Since the APACHE II Data Feed EM requires streaming data, in REDCap to STARR Link's system-level config enter '443' as the 'Starr server port number'.

Project-level REDCap to STARR Link specifics:
- REDCap to STARR Link should be configured to sync with REDCap as the source:
  - The STARR Identifier field should be configured to a field containing MRN.
  - Date field `dt1` should be configured to enrollment date should be configured.
- The APACHE II Data Feed EM requires calling the REDCap to STARR Link API using these queries:
  - `apache2_flowlabs`
  - `apache2_aao2`
  - Copies of these queries are in the `/REDCaptoSTARRLinkSQL` folder.

### REDCap Data Dictionary for APACHE II Data Feed EM
The results of the APACHE II Data Feed EM data querying/processing are mapped to fields with a `tdsr_apache2` prefix.
REDCap projects need this EM's provided data dictionary to be added to their own project's data dictionary in order to work.

The data dictionary includes calculated fields that generate the actual APACHE II scoring.
The calculated fields rely on the actual parameter values fetched by this EM and required manual phenotyping (i.e., APACHE II parameters that aren't automated by this EM).

## How to install and set up in local development environment
Place a copy of the EM into your local REDCap server's directory for external modules.
Make sure the folder's name ends with a '_v9.9.9' suffix (e.g., `apache2-data-feed_v9.9.9`).

### System-Level Configuration
No system-level configuration is necessary.

### Project-Level Configuration
No project-level configuration is necessary. However, it is helpful to enable emLogger.
