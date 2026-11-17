-- Run in the FHIR_Lakehouse SQL analytics endpoint after its tables sync.
CREATE OR ALTER VIEW gold.vw_fhir_patient AS
SELECT resource_id AS patient_id, gender, source_last_updated,
       extraction_timestamp, api_url_or_params, raw_saved_timestamp,
       silver_saved_timestamp, source_version_id, valid_from, valid_to, is_current
FROM silver.fhir_patient
WHERE is_current = 1;
GO

CREATE OR ALTER VIEW gold.vw_fhir_encounter AS
SELECT e.resource_id AS encounter_id, e.patient_id, e.status,
       e.encounter_start_utc, e.encounter_start_text,
       CASE WHEN p.patient_id IS NULL THEN 0 ELSE 1 END AS patient_in_sample,
       e.source_last_updated, e.extraction_timestamp, e.api_url_or_params, e.raw_saved_timestamp,
       e.silver_saved_timestamp, e.source_version_id, e.valid_from, e.valid_to, e.is_current
FROM silver.fhir_encounter e
LEFT JOIN gold.vw_fhir_patient p ON p.patient_id = e.patient_id
WHERE e.is_current = 1;
GO

CREATE OR ALTER VIEW gold.vw_fhir_observation AS
SELECT o.resource_id AS observation_id, o.patient_id, o.encounter_id, o.status,
       o.code_system, o.code, o.effective_at_utc, o.effective_datetime_text,
       TRY_CONVERT(decimal(28, 8), o.value_quantity_text) AS value_quantity, o.value_unit,
       CASE WHEN p.patient_id IS NULL THEN 0 ELSE 1 END AS patient_in_sample,
       CASE WHEN e.encounter_id IS NULL THEN 0 ELSE 1 END AS encounter_in_sample,
       o.source_last_updated, o.extraction_timestamp, o.api_url_or_params, o.raw_saved_timestamp,
       o.silver_saved_timestamp, o.source_version_id, o.valid_from, o.valid_to, o.is_current
FROM silver.fhir_observation o
LEFT JOIN gold.vw_fhir_patient p ON p.patient_id = o.patient_id
LEFT JOIN gold.vw_fhir_encounter e ON e.encounter_id = o.encounter_id
WHERE o.is_current = 1;
GO

CREATE OR ALTER VIEW gold.vw_fhir_condition AS
SELECT c.resource_id AS condition_id, c.patient_id, c.encounter_id,
       c.status AS clinical_status, c.code_system, c.code,
       CASE WHEN p.patient_id IS NULL THEN 0 ELSE 1 END AS patient_in_sample,
       CASE WHEN e.encounter_id IS NULL THEN 0 ELSE 1 END AS encounter_in_sample,
       c.source_last_updated, c.extraction_timestamp, c.api_url_or_params, c.raw_saved_timestamp,
       c.silver_saved_timestamp, c.source_version_id, c.valid_from, c.valid_to, c.is_current
FROM silver.fhir_condition c
LEFT JOIN gold.vw_fhir_patient p ON p.patient_id = c.patient_id
LEFT JOIN gold.vw_fhir_encounter e ON e.encounter_id = c.encounter_id
WHERE c.is_current = 1;
GO

CREATE OR ALTER VIEW gold.vw_fhir_daily_load AS
SELECT resource_type, source_date, run_id, attempt_id, pages, entries_returned,
       extraction_timestamp, last_raw_saved_timestamp, finished_timestamp
FROM silver.fhir_daily_load;
GO

CREATE OR ALTER VIEW gold.vw_fhir_resource_snapshot AS
SELECT resource_type, observed_versions, current_resources,
       latest_extraction_timestamp, latest_raw_saved_timestamp,
       latest_silver_saved_timestamp, gold_saved_timestamp
FROM gold.fhir_resource_snapshot;
GO
