-- Check requested date coverage, pagination and successful loads.
SELECT * FROM gold.vw_fhir_daily_load ORDER BY source_date, resource_type;

-- Each resource ID must have exactly one current version. Expected result is zero rows.
WITH history AS (
    SELECT 'Patient' AS resource_type, resource_id, is_current FROM silver.fhir_patient
    UNION ALL SELECT 'Encounter', resource_id, is_current FROM silver.fhir_encounter
    UNION ALL SELECT 'Observation', resource_id, is_current FROM silver.fhir_observation
    UNION ALL SELECT 'Condition', resource_id, is_current FROM silver.fhir_condition
)
SELECT resource_type, resource_id
FROM history
GROUP BY resource_type, resource_id
HAVING SUM(CASE WHEN is_current = 1 THEN 1 ELSE 0 END) <> 1;

-- Assess reference coverage without dropping rows from the two-day slice.
SELECT COUNT_BIG(*) AS observations,
       SUM(CAST(patient_in_sample AS bigint)) AS with_patient,
       SUM(CAST(encounter_in_sample AS bigint)) AS with_encounter
FROM gold.vw_fhir_observation;
