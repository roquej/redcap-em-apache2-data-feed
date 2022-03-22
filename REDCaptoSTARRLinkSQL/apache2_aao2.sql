WITH
    pat_map AS
        (
        SELECT
            rsr.redcap_record_id AS redcap_record_id,
            DATE(rsr.dt1) AS enroll_date,
            pat_map.mrn AS mrn,
            pat_map.pat_map_id AS pat_map_id,
            pat_map.pat_name AS pat_name,
            pat_map.first_name AS first_name,
            pat_map.last_name AS last_name,
            pat_map.birth_date AS birth_date
        FROM `som-rit-phi-starr-tools-prod.stride.pat_map` AS pat_map
        RIGHT JOIN EXTERNAL_QUERY('us.starrapi',
            '''select
                    r.starr_record_id as starr_record_id,
                    r.redcap_record_id as redcap_record_id,
                    qp.dt1 as dt1
                from REDCAP_STARR_RECORD as r
                join REDCAP_STARR_QUERY_PARAMS as qp
                    on r.redcap_record_id = qp.redcap_record_id
                where r.link_id = ? and r.status_code = 'A' and qp.link_id = ? ''') AS rsr
            ON pat_map.mrn = rsr.starr_record_id
        ),
    encounter AS
        (
        SELECT
            pat_map.redcap_record_id AS redcap_record_id,
            pat_map.mrn AS mrn,
            enc.pat_map_id AS pat_map_id,
            enc.pat_enc_csn_id AS pat_enc_csn_id,
            pat_map.birth_date AS birth_date,
            pat_map.enroll_date AS enroll_date,
            CASE
                WHEN enc.adt_arrival_time IS NULL THEN enc.hosp_admsn_time
                WHEN DATETIME_DIFF(DATETIME(enc.adt_arrival_time), DATETIME(enc.hosp_admsn_time), SECOND) < 0 THEN enc.adt_arrival_time
                ELSE enc.hosp_admsn_time
            END
            AS earliest_time,
            enc.hosp_admsn_time AS hosp_admsn_time,
            enc.adt_arrival_time AS adt_arrival_time,
            enc.hosp_dischrg_time AS discharge_time
        FROM `som-rit-phi-starr-tools-prod.stride.shc_encounter` AS enc
        LEFT JOIN pat_map AS pat_map
            ON enc.pat_map_id = pat_map.pat_map_id
        WHERE
            enc_type = 'Hospital Encounter'
            AND pt_class IN ('Emergency Services', 'Observation', 'Inpatient')
            AND appt_type IN ('Admission (Admission)', 'Admission (Discharged)')
            AND hosp_admsn_time IS NOT NULL
            AND hosp_dischrg_time IS NOT NULL
            AND
            (
            SAFE.DATE_DIFF(DATE(enroll_date), DATE(adt_arrival_time), DAY) >= 0
            OR
            SAFE.DATE_DIFF(DATE(enroll_date), DATE(hosp_admsn_time), DAY) >= 0
            )
          AND SAFE.DATE_DIFF(DATE(enroll_date), DATE(hosp_dischrg_time), DAY) <= 0
        ),
    adt AS
        (
        SELECT
            enc.redcap_record_id AS redcap_record_id,
            enc.pat_map_id AS pat_map_id,
            clarity_adt.pat_enc_csn_id AS pat_enc_csn_id,
            clarity_adt.event_time AS event_time,
            clarity_adt.pat_lvl_of_care_c AS level_of_care
        FROM `som-rit-phi-starr-prod.shc_clarity_filtered_latest.clarity_adt` AS clarity_adt
        INNER JOIN encounter AS enc
            ON clarity_adt.pat_enc_csn_id = enc.pat_enc_csn_id
        ORDER BY SAFE_CAST(redcap_record_id AS NUMERIC), event_time ASC
        ),
    adt_physical AS
        (
        SELECT
            enc.redcap_record_id AS redcap_record_id,
            enc.pat_map_id AS pat_map_id,
            shc_adt.pat_enc_csn_id AS pat_enc_csn_id,
            shc_adt.event_time AS event_time,
            CASE
                WHEN
                    department_name IN ('D2ICU-SURGE', 'E2-ICU', 'E29-ICU', 'J4', 'K4', 'L4', 'M4', 'VCP CCU 1', 'VCP CCU 2', 'VC CRITICAL CARE SPECIALTY', 'VCP SURGE PACU')
                    OR pat_service IN ('Critical Care', 'Neurocritical Care', 'Cardiovascular ICU', 'Emergency Critical Care')
                    OR pat_lv_of_care = 'Critical Care'
                THEN '8'
                ELSE '99'
            END
            AS level_of_care
        FROM `som-rit-phi-starr-tools-prod.stride.shc_adt` AS shc_adt
        INNER JOIN encounter AS enc
            ON shc_adt.pat_enc_csn_id = enc.pat_enc_csn_id
        ORDER BY SAFE_CAST(redcap_record_id AS NUMERIC), event_time ASC
        ),
    adt_icu AS
        (
        SELECT * FROM adt WHERE level_of_care = '8'
        UNION DISTINCT
        SELECT * FROM adt_physical WHERE level_of_care = '8'
        ORDER BY SAFE_CAST(redcap_record_id AS NUMERIC)
        ),
    icu_admit AS
        (
        SELECT
            adt_icu.redcap_record_id AS redcap_record_id,
            adt_icu.pat_map_id AS pat_map_id,
            adt_icu.pat_enc_csn_id AS pat_enc_csn_id,
            MIN(event_time) AS icu_admit_dttm,
        FROM adt_icu
        GROUP BY redcap_record_id, pat_enc_csn_id, pat_map_id
        ORDER BY SAFE_CAST(redcap_record_id AS NUMERIC)
        ),
    fio2 AS
        (
        SELECT
            icu_admit.redcap_record_id AS redcap_record_id,
            'FiO2' AS type,
            flow.recorded_time AS dttm,
            CASE
                WHEN SAFE_CAST(flow.meas_value AS DECIMAL) > 100 THEN NULL
                WHEN SAFE_CAST(flow.meas_value AS DECIMAL) >= 21 THEN SAFE_CAST(flow.meas_value AS DECIMAL) / 100
                WHEN SAFE_CAST(flow.meas_value AS DECIMAL) <= 1 AND SAFE_CAST(flow.meas_value AS DECIMAL) >= 0.21 THEN SAFE_CAST(flow.meas_value AS DECIMAL)
                ELSE NULL
            END AS value
        FROM `som-rit-phi-starr-tools-prod.stride.rit_flowsheet` AS flow
        INNER JOIN icu_admit AS icu_admit
            ON flow.pat_map_id = icu_admit.pat_map_id
        WHERE
            row_disp_name IN ('FiO2 (%)', 'O2 % Concentration')
            AND SAFE.DATE_DIFF(flow.recorded_time, icu_admit.icu_admit_dttm, SECOND) >= 0
            AND SAFE.DATE_DIFF(flow.recorded_time, DATETIME_ADD(icu_admit.icu_admit_dttm, INTERVAL 24 HOUR), SECOND) <= 0
        ),
    abg_labs AS
        (
        SELECT
            icu_admit.redcap_record_id AS redcap_record_id,
            CASE
                WHEN labs.lab_name IN ('PCO2, ISTAT',
                                  'Arterial pCO2 for POC',
                                  'pCO2 (a)',
                                  'pCO2 (a), ISTAT',
                                  'PCO2') THEN 'PaCO2'
                WHEN labs.lab_name IN ('pO2 (a)',
                                  'PO2, ISTAT',
                                  'PO2',
                                  'Arterial pO2 for POC',
                                  'PO2 (a), ISTAT') THEN 'PaO2'
            END AS type,
            labs.taken_time AS dttm,
            SAFE_CAST(labs.ord_num_value AS DECIMAL) AS value
        FROM `som-rit-phi-starr-tools-prod.stride.shc_lab_result` AS labs
        INNER JOIN icu_admit AS icu_admit
            ON labs.pat_map_id = icu_admit.pat_map_id
        WHERE
            lab_name IN (
                        'PCO2, ISTAT',
                        'Arterial pCO2 for POC',
                        'pCO2 (a)',
                        'pCO2 (a), ISTAT',
                        'PCO2',
                        'pO2 (a)',
                        'PO2, ISTAT',
                        'PO2',
                        'Arterial pO2 for POC',
                        'PO2 (a), ISTAT'
                        )
            AND SAFE.DATE_DIFF(labs.taken_time, icu_admit.icu_admit_dttm, SECOND) >= 0
            AND SAFE.DATE_DIFF(labs.taken_time, DATETIME_ADD(icu_admit.icu_admit_dttm, INTERVAL 24 HOUR), SECOND) <= 0
          AND SAFE_CAST(labs.ord_num_value AS DECIMAL) < 9999999
          AND SAFE_CAST(labs.ord_num_value AS DECIMAL) IS NOT NULL -- checks if ord_num_value column is numeric
                                                                   -- instead of dropping nonnumeric ord_num_value rows, consider using regex instead
        ),
    apache2_aao2 AS
        (
        SELECT * FROM fio2
        UNION ALL
        SELECT * FROM abg_labs
        )
SELECT DISTINCT *
FROM apache2_aao2
WHERE
    redcap_record_id IS NOT NULL
    AND type IS NOT NULL
    AND dttm IS NOT NULL
    AND value IS NOT NULL
ORDER BY
    SAFE_CAST(redcap_record_id AS DECIMAL), dttm ASC