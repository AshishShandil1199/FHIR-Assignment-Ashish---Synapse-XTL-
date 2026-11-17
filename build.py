"""Regenerate the importable notebooks and Fabric pipeline definition."""
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parent
BASE = "https://hapi.fhir.org/baseR4"
RESOURCES = ("Patient", "Encounter", "Observation", "Condition")


def cell(kind, source, **meta):
    if not source.endswith("\n"):
        source += "\n"
    value = {"cell_type": kind, "metadata": meta, "source": source.splitlines(keepends=True)}
    if kind == "code":
        value.update(execution_count=None, outputs=[])
    return value


def write_json(path, obj):
    output = ROOT / path
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(obj, indent=2) + "\n")


def notebook(name, title, params, code_sections, command):
    param_code = "\n".join(f"{key} = {val!r}" for key, val in params.items()) + "\n"
    cells = [cell("markdown", title)]
    if params:
        cells.append(cell("code", param_code, tags=["parameters"]))
    for heading, source in code_sections:
        cells.extend([cell("markdown", heading), cell("code", source)])
    cells.append(cell("code", command))
    write_json(f"notebooks/{name}.ipynb", {
        "nbformat": 4, "nbformat_minor": 4,
        "metadata": {
            "kernelspec": {"display_name": "PySpark (Python)", "language": "python", "name": "synapse_pyspark"},
            "language_info": {"name": "python"}
        },
        "cells": cells
    })


def expression(value):
    return {"value": value, "type": "Expression"}


def notebook_activity(name, previous, notebook_id, params):
    return {
        "name": name, "type": "TridentNotebook",
        "dependsOn": [{"activity": previous, "dependencyConditions": ["Succeeded"]}] if previous else [],
        "policy": {"timeout": "0.02:00:00", "retry": 1, "retryIntervalInSeconds": 30},
        "typeProperties": {
            "workspaceId": "<WORKSPACE_ID>", "notebookId": notebook_id,
            "parameters": {name: {"value": value, "type": "string"} for name, value in params.items()}
        }
    }


def main():
    http_source = (ROOT / "fhir_extract.py").read_text()
    delta_source = (ROOT / "fhir_delta.py").read_text()
    _, bronze_and_silver = delta_source.split("PAGE_SCHEMA =", 1)
    bronze_source, silver_and_gold = bronze_and_silver.split("def silver_history(", 1)
    silver_source, gold_source = silver_and_gold.split("def build_gold(", 1)
    shared = (
        "from pyspark.sql import functions as F\n"
        f"RESOURCES = {RESOURCES!r}\n\n"
        + delta_source[delta_source.index("def table_name("):delta_source.index("def audit_frame(")]
    )
    bronze_imports = delta_source[delta_source.index("from delta.tables"):delta_source.index("PAGE_SCHEMA =")]
    bronze_setup, bronze_steps = bronze_source.split("def bronze_frame(", 1)
    bronze_read, bronze_load = bronze_steps.split("def ingest(", 1)
    http_setup, http_steps = http_source.split("def land_day(", 1)
    http_download, http_hashes = http_steps.split("def canonical_resource(", 1)
    bronze_sections = [
        ("## Imports, dates and API requests", http_setup),
        ("## Download and save each page", "def land_day(" + http_download),
        ("## Identify resource changes", "def canonical_resource(" + http_hashes),
        ("## Set up the Delta tables", bronze_imports + "PAGE_SCHEMA =" + bronze_setup),
        ("## Read the JSON and select the resource fields", "def bronze_frame(" + bronze_read),
        ("## Load each day and record the result", "def ingest(" + bronze_load),
    ]
    notebook("Bronze/FHIR_Ingest", "# FHIR API to Raw and Bronze Delta", {
        "resource_type": "Patient", "start_date": "", "end_date_exclusive": "",
        "backfill_days": "2", "anchor_utc": "", "run_id": "",
        "api_base": BASE, "page_size": "200", "max_pages": "250",
        "raw_root": "/lakehouse/default/Files/fhir/raw"
    }, bronze_sections, """from pathlib import Path

if not Path('/lakehouse/default/Files').is_dir():
    raise RuntimeError('Attach the FHIR Lakehouse as the default Lakehouse')
spark.conf.set('spark.sql.session.timeZone', 'UTC')
days = search_days(start_date, end_date_exclusive, backfill_days, anchor_utc)
if not run_id:
    run_id = uuid4().hex
page_size = int(page_size)
max_pages = int(max_pages)
ingest(spark, raw_root, resource_type,
       days, run_id, api_base, page_size, max_pages,
       read_root="Files/raw")
""")
    notebook("Silver/FHIR_Silver", "# FHIR Silver SCD Type 2", {"api_base": BASE},
             [("## Spark SQL history and PySpark resource columns",
               shared + "def silver_history(" + silver_source)], """spark.conf.set('spark.sql.session.timeZone', 'UTC')
build_silver(spark, api_base)
""")
    notebook("Gold/FHIR_Gold", "# FHIR Gold Resource Snapshot", {},
             [("## Spark SQL resource aggregates", shared + "def build_gold(" + gold_source)], """spark.conf.set('spark.sql.session.timeZone', 'UTC')
build_gold(spark)
""")
    defaults = {"start_date": "", "end_date_exclusive": "", "backfill_days": "2",
                "api_base": BASE, "page_size": "200", "max_pages": "250"}
    passed = {p: expression(f"@pipeline().parameters.{p}") for p in defaults}
    passed.update(run_id=expression("@pipeline().RunId"), anchor_utc=expression("@pipeline().TriggerTime"))
    activities = []
    previous = None
    for resource in RESOURCES:
        name = f"Ingest_{resource}"
        activities.append(notebook_activity(name, previous, "<INGEST_NOTEBOOK_ID>",
                                            {"resource_type": resource, **passed}))
        previous = name
    activities.append(notebook_activity("Build_Silver_History", previous, "<SILVER_NOTEBOOK_ID>",
                                        {"api_base": passed["api_base"]}))
    activities.append(notebook_activity("Build_Gold_Snapshot", "Build_Silver_History",
                                        "<GOLD_NOTEBOOK_ID>", {}))
    write_json("pipeline/pipeline-content.json", {"properties": {
        "parameters": {p: {"type": "String", "defaultValue": val} for p, val in defaults.items()},
        "activities": activities
    }})


if __name__ == "__main__":
    main()
