# FHIR Fabric assignment submission

## Deliverables

| Asset | Files | Purpose |
| --- | --- | --- |
| Source code | [fhir_extract.py](fhir_extract.py), [fhir_delta.py](fhir_delta.py) | HTTP ingestion, pagination, raw landing, Delta merges, SCD2, Gold snapshot |
| Notebook generator | [build.py](build.py) | Rebuilds the portable notebooks and six-activity pipeline from source |
| Bronze notebook | [FHIR_Ingest.ipynb](notebooks/Bronze/FHIR_Ingest.ipynb) | Parameterized ingestion for one resource type |
| Silver notebook | [FHIR_Silver.ipynb](notebooks/Silver/FHIR_Silver.ipynb) | Rebuilds observed business-change history |
| Gold notebook | [FHIR_Gold.ipynb](notebooks/Gold/FHIR_Gold.ipynb) | Publishes resource counts and freshness |
| Pipeline setup | [pipeline-content.json](pipeline/pipeline-content.json), [make_create_request.py](pipeline/make_create_request.py) | Portable definition and UUID substitution/request-body helper |
| Dataflow transform | [daily_ingestion_coverage.pq](dataflow/daily_ingestion_coverage.pq) | Daily successful-load coverage; destination must be configured in Fabric |
| SQL | [gold_views.sql](sql/gold_views.sql), [validate.sql](sql/validate.sql) | Six reporting views and quality checks |
| Tests | [test_ingestion.py](tests/test_ingestion.py), [local_delta.py](tests/local_delta.py) | Offline unit tests and optional Spark/Delta regression |
| Historical evidence | [validation_counts.csv](validation_counts.csv) | Aggregate counts for the original two-day validation, not current totals |
| Deployment guide | [README.md](README.md) | Capacity, Lakehouse, shortcut, notebook, pipeline and Dataflow setup |

This is a portable local-source submission, not a byte-for-byte export of the
current Fabric workspace. Notebook source is included without execution outputs.
No patient records, raw responses, access tokens, credentials, virtual environment,
editor configuration, or code-index database are included.

## Pipeline and data lineage

```mermaid
flowchart TD
    API["FHIR R4 search API: UTC lastUpdated window"]
    IP["Ingest_Patient"]
    IE["Ingest_Encounter"]
    IO["Ingest_Observation"]
    IC["Ingest_Condition"]
    RAW["Files/fhir/raw: original response bytes"]
    SHORTCUT["Files/raw: OneLake shortcut to raw landing"]
    BRONZE["bronze.fhir_patient / encounter / observation / condition"]
    AUDIT["bronze.fhir_page_audit + bronze.fhir_run_audit"]
    SILVER["Build_Silver_History: four silver.fhir_* SCD2 tables"]
    GOLD["Build_Gold_Snapshot: gold.fhir_resource_snapshot"]
    DF["Refresh_Daily_Coverage: configured Dataflow Gen2"]
    DAILY["silver.fhir_daily_load"]
    VIEWS["Gold SQL endpoint reporting views"]
    PBI["FHIR Gold Overview: snapshot only"]
    API --> IP --> IE --> IO --> IC
    IP --> RAW
    IE --> RAW
    IO --> RAW
    IC --> RAW
    RAW --> SHORTCUT --> BRONZE
    IP --> AUDIT
    IE --> AUDIT
    IO --> AUDIT
    IC --> AUDIT
    IC --> SILVER
    BRONZE --> SILVER --> GOLD --> PBI
    GOLD --> DF
    AUDIT --> DF --> DAILY
    SILVER --> VIEWS
    GOLD --> VIEWS
    DAILY --> VIEWS
```

The JSON definition has **six Notebook activities**, linked by successful
completion, with one retry and a 30-second retry interval. It intentionally has
placeholder IDs. Import the notebooks first, bind their default Lakehouse, then
render the request with the helper described in [README.md](README.md).

The optional seventh activity, Dataflow refresh, is **not in the portable JSON**.
Create the Dataflow, configure its connection and Replace destination, then add
the activity after Gold. Without this step, the daily-load table and its reporting
view will not be available. Run SQL view creation after the tables synchronize;
create the daily-load view only after its destination exists.

All four ingestion activities receive the same dates, pipeline RunId and trigger
time. Explicit start dates are inclusive; end dates are exclusive. Blank date
bounds select the preceding complete UTC days. Run the pipeline serially:
Silver and Gold overwrite shared derived tables.

## Table grains, keys and relationships

| Object | Grain / logical key | Parent or source |
| --- | --- | --- |
| Raw JSON pages | Resource, source date, run, attempt, page number | FHIR search response |
| `bronze.fhir_page_audit` | `(attempt_id, page_number)` | Exact request URL and raw file path |
| `bronze.fhir_run_audit` | `(resource_type, source_date, run_id, started_timestamp)` | Completion/failure of a resource-day attempt |
| Four `bronze.fhir_*` tables | Canonical resource event, deduplicated by `event_id` within each table | Matching entries in raw pages |
| Four `silver.fhir_*` tables | Business-change version; `(resource_id, valid_from)` within each table | Corresponding Bronze history |
| `gold.vw_fhir_patient` | One current row per `patient_id` | Current Silver Patient |
| `gold.vw_fhir_encounter` | One current row per `encounter_id` | Current Silver Encounter; optional Patient |
| `gold.vw_fhir_observation` | One current row per `observation_id` | Current Silver Observation; optional Patient and Encounter |
| `gold.vw_fhir_condition` | One current row per `condition_id` | Current Silver Condition; optional Patient and Encounter |
| `silver.fhir_daily_load` / `gold.vw_fhir_daily_load` | `(resource_type, source_date)` for the latest successful attempt | Dataflow joins page and successful run audits |
| `gold.fhir_resource_snapshot` / `gold.vw_fhir_resource_snapshot` | One row per `resource_type` | Counts and timestamp maxima over Silver |

The clinical relationships below apply to the **current Gold views**, not to
unfiltered SCD2 history. They are logical joins, not enforced SQL foreign keys.

```mermaid
erDiagram
    PATIENT_CURRENT o|--o{ ENCOUNTER_CURRENT : patient_id
    PATIENT_CURRENT o|--o{ OBSERVATION_CURRENT : patient_id
    PATIENT_CURRENT o|--o{ CONDITION_CURRENT : patient_id
    ENCOUNTER_CURRENT o|--o{ OBSERVATION_CURRENT : encounter_id
    ENCOUNTER_CURRENT o|--o{ CONDITION_CURRENT : encounter_id
    PATIENT_CURRENT {
        string patient_id PK
    }
    ENCOUNTER_CURRENT {
        string encounter_id PK
        string patient_id FK
    }
    OBSERVATION_CURRENT {
        string observation_id PK
        string patient_id FK
        string encounter_id FK
    }
    CONDITION_CURRENT {
        string condition_id PK
        string patient_id FK
        string encounter_id FK
    }
```

- `subject.reference` supplies `patient_id`; `encounter.reference` supplies
  `encounter_id`. Relative references and absolute references under the configured
  API base are normalized; optional `/_history/<version>` suffixes are supported.
- A date-window extract is not a full clinical population. Parent records can be
  absent. Views use left joins and expose `patient_in_sample` and, where applicable,
  `encounter_in_sample`; they do not discard unmatched children.
- Never join full Silver histories using `resource_id` alone: multiple versions
  can multiply rows. Filter `is_current = 1`, or use an explicit as-of interval
  `valid_from <= timestamp < valid_to`, treating a null end as open-ended.
- Bronze retains metadata-only events; Silver only creates a new interval when
  the business content hash changes. `valid_to` is exclusive and each observed
  resource has one current row. FHIR search does not recover missed intermediate
  versions or deletions.
- Audit joins use `(resource_type, source_date, run_id, attempt_id)`. Daily coverage
  selects the latest successful complete attempt, not the sum of all retries.
- Snapshot counts and daily activity have different grains. Do not join the
  snapshot to clinical rows or sum it after a one-to-many join.

## Power BI handoff

- [Fabric workspace](https://app.fabric.microsoft.com/groups/b6659a9d-9437-4334-80d8-b2e649a22ab9/list?experience=fabric-developer)
- [Saved one-page FHIR Gold Overview report](https://app.fabric.microsoft.com/groups/b6659a9d-9437-4334-80d8-b2e649a22ab9/reports/a6843707-cdfe-4edf-9d47-cf7bd9e7f12f/b646f14a64700705d319?experience=fabric-developer)
- Semantic model: **FHIR Gold Analytics**, Direct Lake over
  `gold.fhir_resource_snapshot` only. It has no clinical-table relationships.

The page contains categorized resource-count cards, a count-by-resource bar
chart, a composition donut and a counts/freshness table. The corrected cards show
separate entity counts; **81,374 is the total across all four types**, not the
count of each type. The last browser-verified snapshot was:

| Resource | Current resources |
| --- | ---: |
| Patient | 1,398 |
| Encounter | 641 |
| Observation | 65,997 |
| Condition | 13,338 |
| Total | 81,374 |

These are observed report results, not hardcoded measures, and will change with
the source. They are separate from the historical 7,180-event validation.

Model measures:

```dax
Current Resources = SUM('fhir_resource_snapshot'[current_resources])
Observed Versions = SUM('fhir_resource_snapshot'[observed_versions])
Resource Types = DISTINCTCOUNT('fhir_resource_snapshot'[resource_type])
Resource Count Display = FORMAT([Current Resources], "#,0")
```

Use the text display measure only for exact-count cards categorized by
`resource_type`; use the numeric measure for charts and aggregation. The duplicate
Observed Versions column was removed from the report table.

**Boundary:** this ZIP includes report links and reconstruction details, but no
PBIX/PBIP report export or semantic-model definition export. Opening the live
report requires workspace permission. The report is a resource overview, not a
multi-table clinical analysis. The six portable SQL views are supplied separately;
additional view names seen in the live workspace are not claimed as included.

## Validation and reproducibility

From the project root on Windows, the offline tests require only Python 3.10+
and the standard library:

```powershell
python -m unittest discover -s tests -p "test_*.py" -v
```

For intentional regeneration of the portable assets:

```powershell
python build.py
```

Regeneration overwrites the three local notebooks and pipeline JSON. Preserve
any manual changes first; it does not deploy to Fabric.

The optional integration regression in [tests/local_delta.py](tests/local_delta.py)
requires a working Spark/JVM environment, PySpark 3.5.3 and Delta Lake 3.2.1.
Set `DELTA_JARS` to the required local Delta Spark and Delta storage JAR paths
(comma-separated) and put the project root on `PYTHONPATH` before running
`python tests\local_delta.py`. Fabric supplies its own Spark/Delta runtime for
the deployed notebooks. The optional live mode requires external raw fixtures;
they are intentionally not distributed.

Packaging validation reruns the five offline tests, checks Python and notebook
cell syntax, checks that generated assets match source, validates the pipeline
chain and parameters, and verifies the ZIP content and checksums. This does not
replace a fresh Fabric pipeline run or SQL endpoint validation after deployment.

## GitHub submission

Submission repository:
[FHIR-Assignment-Ashish---Synapse-XTL-](https://github.com/AshishShandil1199/FHIR-Assignment-Ashish---Synapse-XTL-).
Its source files contain the same portable implementation supplied in the ZIP,
with the repository heading preserved and this handoff section updated.
Repository visibility is not changed by the upload.

To retrieve the submission:

```powershell
git clone https://github.com/AshishShandil1199/FHIR-Assignment-Ashish---Synapse-XTL-.git
Set-Location FHIR-Assignment-Ashish---Synapse-XTL-
python -m unittest discover -s tests -p "test_*.py" -v
```

For future updates, commit and push to the existing repository. Authenticate
using GitHub's normal credential flow; never paste credentials into source files.
Do not upload raw patient data, secrets, or local notebook outputs.
