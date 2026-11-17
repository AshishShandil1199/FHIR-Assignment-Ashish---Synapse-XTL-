# FHIR Data Pipeline on Microsoft Fabric

The pipeline loads **Patient → Encounter → Observation → Condition**, then builds Silver history and a Gold resource snapshot. Bronze reads each landed JSON page as text through a OneLake Files shortcut. It uses basic PySpark steps to parse the Bundle, expand its entries, filter matching resources and select their fields. The ingestion notebook separates downloading, table setup, reading and loading into individual cells. Silver uses Spark SQL window functions for SCD Type 2 and PySpark for resource columns. Gold uses Spark SQL for reporting aggregates. The Bronze HTTP requests and raw-byte writes use Python, and a Spark UDF preserves the canonical JSON hashes needed by Silver. Pagination, validation, audit tables and duplicate prevention are retained. A Dataflow Gen2 transforms Bronze audit tables into daily ingestion coverage for reporting.

## Workspace layout

The Bronze notebook favors readability over tuning: ordinary loops handle pagination and counts, table schemas list their columns explicitly, and PySpark transformations run as separate statements. It does not add caching, repartitioning or broadcast hints. The Delta merge and canonical hash calculation remain because replay protection and Silver history depend on them.

| Workspace folder | Notebook | Lakehouse outputs |
| --- | --- | --- |
| Bronze | `FHIR_Ingest` | Raw bundles in `Files/fhir/raw`, four `bronze.fhir_*` Delta tables, `bronze.fhir_page_audit`, `bronze.fhir_run_audit` |
| Silver | `FHIR_Silver` | Four `silver.fhir_*` SCD Type 2 Delta tables |
| Gold | `FHIR_Gold` | `gold.fhir_resource_snapshot`. Install the reporting views in the SQL analytics endpoint |
| Workspace root | `FHIR_Incremental_Load` pipeline and `FHIR_Daily_Ingestion_Coverage` dataflow | Dataflow destination `silver.fhir_daily_load` |

Raw files use `Files/fhir/raw/<Resource>/date=<YYYY-MM-DD>/<run_id>/<attempt_id>/page-000001.json`. Every complete response is written byte for byte before Bronze conversion. The page audit records the exact request URL, request start, raw save time, file path and matching entries. The daily run audit records load status and the finishing timestamp. Failed attempts retain landed pages for diagnosis. Bronze merges on the canonical resource event ID, so retries of a complete day avoid duplicate events. Page audits retain repeated requests for operational traceability.

## OneLake shortcut

In `FHIR_Lakehouse`, the internal OneLake shortcut `Files/raw` points to the physical folder `Files/fhir/raw` in the same Lakehouse. The API client writes the original response bytes into the physical folder. PySpark reads those bundles through `Files/raw`, using the same resource and date partitions beneath both paths. The shortcut uses passthrough identity and references the existing files.

For a fresh deployment, create the folders `Files/fhir/raw` in the Lakehouse, then use **Files → New shortcut → Microsoft OneLake**. Select `FHIR_Lakehouse`, choose the `Files/fhir/raw` folder, and create the shortcut named `raw` directly under `Files`. Create the shortcut before starting the pipeline.

## Deploy and run

1. Assign the Fabric workspace to an active Fabric capacity or Fabric trial and make sure Dataflow Gen2 is enabled. You need permission to create items. The deployment in the linked workspace uses the existing Bronze, Silver and Gold folders.
2. Create a Lakehouse named `FHIR_Lakehouse` with **Lakehouse schemas** enabled. Under **Tables → … → New schema**, create `bronze`, `silver` and `gold`. The notebooks also ensure their respective schemas exist. The system-provided `dbo` schema will be present.
3. Use **Import → Notebook** to import the three notebooks from their matching folders in this ZIP. If imported at workspace root, use each notebook's **Move to** action. Attach and pin `FHIR_Lakehouse` as the default in all three. In `FHIR_Ingest` and `FHIR_Silver`, verify that the first code cell is marked **Parameters**. If the import doesn't preserve the tag, use the cell menu and select **Toggle parameter cell**.
4. Create a pipeline named `FHIR_Incremental_Load` and add Notebook activities in the following order, linked by **On success**. Set retry to 1 and retry interval to 30 seconds.

   | Activity | Notebook | Base parameters |
   | --- | --- | --- |
   | Ingest_Patient | FHIR_Ingest | `resource_type = Patient` plus shared parameters below |
   | Ingest_Encounter | FHIR_Ingest | `resource_type = Encounter` plus shared parameters |
   | Ingest_Observation | FHIR_Ingest | `resource_type = Observation` plus shared parameters |
   | Ingest_Condition | FHIR_Ingest | `resource_type = Condition` plus shared parameters |
   | Build_Silver_History | FHIR_Silver | `api_base = @pipeline().parameters.api_base` |
   | Build_Gold_Snapshot | FHIR_Gold | None |
   | Refresh_Daily_Coverage | Dataflow activity | FHIR_Daily_Ingestion_Coverage. Add after the first load and dataflow setup below |

   Create the string parameters below. Map each to the same-named Notebook base parameter using dynamic content `@pipeline().parameters.<name>`. Also pass `run_id = @pipeline().RunId` and `anchor_utc = @pipeline().TriggerTime` as dynamic content to all four ingestion activities.

   | Parameter | Default | First validation run |
   | --- | --- | --- |
   | `start_date` | Empty | `2026-11-11` |
   | `end_date_exclusive` | Empty | `2026-11-13` |
   | `backfill_days` | `2` | `2` |
   | `api_base` | `https://hapi.fhir.org/baseR4` | Default |
   | `page_size` | `200` | Default |
   | `max_pages` | `250` | Default |

   For REST deployment of the six Notebook activities, use the following command, then send the generated body to `POST https://api.fabric.microsoft.com/v1/workspaces/{workspaceId}/dataPipelines` with authorized Fabric authentication.

   ```bash
   python pipeline/make_create_request.py --workspace WORKSPACE_GUID \
     --ingest INGEST_NOTEBOOK_GUID --silver SILVER_NOTEBOOK_GUID \
     --gold GOLD_NOTEBOOK_GUID > pipeline-request.json
   ```

5. Run the pipeline with the validation dates and confirm all six activities succeed. The four ingestion activities use the same UTC window. With blank dates the pipeline reprocesses the preceding complete UTC days relative to its trigger time. Use explicit date bounds to recover missed windows. Run these pipelines serially because Silver and Gold rebuild shared snapshots.
6. Create `FHIR_Daily_Ingestion_Coverage` as a Dataflow Gen2. Paste `dataflow/daily_ingestion_coverage.pq` into a blank query's Advanced Editor, replace the workspace and lakehouse IDs, and authenticate the Lakehouse connection. If navigation differs in your tenant, select the audit tables through the Lakehouse connector with **Navigate using full hierarchy** and reuse the generated navigation steps. Configure a Lakehouse destination for `silver.fhir_daily_load` with **Replace**. Save, run a refresh, then append the Dataflow activity after Gold and select this dataflow. The transform selects the newest successful attempt for each resource and UTC date.
7. Open the Lakehouse SQL analytics endpoint after table metadata synchronizes. Run `sql/gold_views.sql`, executing one statement at a time if the editor doesn't parse `GO`. Run `sql/validate.sql`. Confirm a second pipeline run keeps the same Bronze event counts when the source data is unchanged.

## Table relationships and version semantics

- Each Silver table uses the FHIR `resource_id` as its business key. `valid_from` is the source update timestamp and `valid_to` is exclusive. Each resource has one current row. The content hash excludes `meta.versionId` and `meta.lastUpdated`, so a metadata-only version bump won't create an SCD row. The Bronze history is re-evaluated in source order whenever an earlier day is replayed.
- Patient is the parent of Encounter, Observation and Condition via their `subject.reference`. Observation and Condition can also reference Encounter. Gold current views keep unmatched references because a related resource might not have changed within the requested days. The first FHIR coding is surfaced for simple analytics, and Silver retains the source JSON and metadata.
- FHIR search returns searchable versions visible at fetch time. The SCD history captures versions observed by the pipeline. It cannot recover deleted resources or intermediate versions missed between searches. UTC dates refer to `meta.lastUpdated`, rather than clinical encounter or observation dates.
- The Gold snapshot has one row per resource type. The daily coverage view has one row per resource type and UTC date. Per-record lineage is available in the Gold current views and Silver history.

## Verification

Integration validation used live HAPI R4 Bundle responses dated **11–12 November 2026 UTC** with Spark 3.5.3 and Delta Lake 3.2.1. The counts below describe that validation window, not current workspace totals. See [validation_counts.csv](validation_counts.csv). Source records and notebook execution outputs are excluded from the repository.

| Resource | Pages | Returned entries | Bronze events | Silver versions |
| --- | ---: | ---: | ---: | ---: |
| Patient | 3 | 356 | 356 | 356 |
| Encounter | 3 | 435 | 435 | 435 |
| Observation | 32 | 6,076 | 6,076 | 6,076 |
| Condition | 3 | 313 | 313 | 313 |
| Total | 41 | 7,180 | 7,180 | 7,180 |

The original checks verified exact Raw bytes, pagination, duplicate replay, late-arriving versions, an A→B→A change sequence, one current row per resource ID, and the four-row Gold snapshot. At that validation, the Fabric workspace contained the Lakehouse, the three notebooks, the pipeline, the Dataflow Gen2 and all six SQL reporting views. Fabric SQL validation returned 8 resource-day rows, 41 pages, 7,180 entries and zero resource IDs with an invalid number of current rows.

The previously validated live pipeline used one parameterized Bronze notebook activity to ingest the four resources in order, followed by Silver, Gold and the Dataflow refresh. The portable pipeline definition uses four separate ingestion activities, one per resource. The live Bronze activity was configured with the two verification dates; inspect its current configuration before running. To run a different window, change the two date parameters together. Set both to blank to use the notebook's rolling two-day window.

### References

See [technical documentation](SUBMISSION.md) for the lineage diagram, table grains,
relationship diagram, test commands, and Power BI report scope.

- [FHIR R4 search](https://hl7.org/fhir/R4/search.html)
- [Fabric Lakehouse schemas](https://learn.microsoft.com/en-us/fabric/data-engineering/lakehouse-schemas)
- [Fabric notebook authoring](https://learn.microsoft.com/en-us/fabric/data-engineering/author-execute-notebook)
- [Fabric pipeline definition](https://learn.microsoft.com/en-us/rest/api/fabric/articles/item-management/definitions/datapipeline-definition)
- [Dataflow Gen2 destinations](https://learn.microsoft.com/en-us/fabric/data-factory/dataflow-gen2-data-destinations-and-managed-settings)
- [Dataflow activity](https://learn.microsoft.com/en-us/fabric/data-factory/dataflow-activity)
