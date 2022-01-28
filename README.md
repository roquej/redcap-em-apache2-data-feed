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

[APACHE II Data Feed EM Installation/Configuration Instructions for Researchers](https://www.example.com)

## Dependencies
### REDCap to STARR Link EM and queries to store
REDCap to STARR Link and its dependencies must be installed and configured to the project.
Refer to [DOCUMENT HERE]() for general guidance.

Project-level REDCap to STARR Link specifics:
1. REDCap to STARR Link should be configured to sync with REDCap as the source:
   1. The STARR Identifier field should be configured to a field containing MRN.
   2. Date field `dt1` should be configured to enrollment date should be configured.
2. The APACHE II Data Feed EM requires calling the REDCap to STARR Link API using these queries:
   1. `apache2_flowlabs`
   2. `apache2_aao2`
   3. Copies of these queries are in the `/REDCaptoSTARRLinkSQL` folder.

### REDCap Data Dictionary for APACHE II Data Feed EM
The results of the APACHE II Data Feed EM data querying/processing are mapped to fields with a `tdsr_apache2` prefix.
REDCap projects need this EM's provided data dictionary to be merged with their own projects in order to work.


## How to install and set up in local development environment



