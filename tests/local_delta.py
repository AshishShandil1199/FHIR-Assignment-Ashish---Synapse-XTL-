"""Local regression for Delta deduplication, backfills and A→B→A history."""
import json
import os
import sys
from pathlib import Path
from tempfile import TemporaryDirectory

from pyspark.sql import SparkSession
from fhir_delta import BRONZE_SCHEMA, bronze_frame, build_gold, merge_append, model_silver, silver_history, table_name
from fhir_extract import RESOURCES, canonical_resource, land_day, search_days
from datetime import date, datetime, timezone

def source_bundle(body, next_url=None):
    links = [{"relation": "next", "url": next_url}] if next_url else []
    return json.dumps({"resourceType": "Bundle", "type": "searchset", "link": links,
                       "entry": [{"resource": body}]}, indent=2).encode()

def main(spark, raw):
    base = "https://example.test/fhir"
    day = date(2026, 11, 11)
    assert list(search_days("", "", 2, "2026-11-13T23:01:00+02:00"))[0] == day
    patient = {"resourceType":"Patient", "id":"p1", "gender":"female",
               "meta":{"lastUpdated":f"{day}T01:00:00Z","versionId":"1"}}
    body = source_bundle(patient, "/fhir/Patient?page=2")
    answers = iter([body, source_bundle(patient)])
    pages = land_day(raw, base, "Patient", day, 1, 2, "test", fetch=lambda url: next(answers))
    assert (raw / pages[0]["relative_path"]).read_bytes() == body
    assert len(pages) == 2 and pages[-1]["api_url_or_params"].endswith("page=2")
    rows = bronze_frame(spark, raw, pages)
    assert rows.count() == 2
    expected = canonical_resource(json.dumps(patient))
    row = rows.first()
    assert (row.resource_json, row.content_hash, row.event_id) == expected
    assert row.relative_path in [page["relative_path"] for page in pages]
    empty = bronze_frame(spark, raw, [])
    assert empty.count() == 0 and empty.schema == BRONZE_SCHEMA

    mixed = json.dumps({
        "resourceType": "Bundle", "type": "searchset",
        "entry": [
            {"resource": patient, "search": {"mode": "match"}},
            {"resource": patient, "search": {"mode": "include"}},
            {"resource": {"resourceType": "OperationOutcome"}},
        ],
    }, indent=2).encode()
    mixed_pages = land_day(raw, base, "Patient", day, 1, 1, "mixed", fetch=lambda url: mixed)
    assert bronze_frame(spark, raw, mixed_pages).count() == 1

    no_matches = json.dumps({"resourceType": "Bundle", "type": "searchset", "entry": []}).encode()
    empty_pages = land_day(raw, base, "Patient", day, 1, 1, "empty", fetch=lambda url: no_matches)
    assert bronze_frame(spark, raw, empty_pages).count() == 0
    all_pages = pages.copy()
    for suffix, (stamp, gender) in enumerate([("02", "female"), ("03", "other"), ("04", "female")], 2):
        patient["meta"] = {"lastUpdated": f"{day}T{stamp}:00:00Z", "versionId":str(suffix)}
        patient["gender"] = gender
        later = land_day(raw, base, "Patient", day, 1, 1, "test", fetch=lambda url:source_bundle(patient))
        all_pages.extend(later)
    table = "bronze.fhir_patient"
    spark.sql("CREATE SCHEMA IF NOT EXISTS bronze")
    merge_append(spark, table, BRONZE_SCHEMA, bronze_frame(spark, raw, later), ["event_id"])
    merge_append(spark, table, BRONZE_SCHEMA, bronze_frame(spark, raw, all_pages), ["event_id"])
    bronze = spark.table(table)
    assert bronze.count() == 4
    silver = model_silver(silver_history(spark, bronze), "Patient", base)
    observed = [(r.gender, r.valid_from.hour, r.valid_to.hour if r.valid_to else None, r.is_current)
                for r in silver.orderBy("valid_from").collect()]
    assert observed == [("female", 1, 3, False), ("other", 3, 4, False), ("female", 4, None, True)], observed
    print("PASS: pagination, exact Raw bytes, replay, late backfill and SCD2 A→B→A", flush=True)


def live(spark, folder):
    spark.sql("CREATE SCHEMA IF NOT EXISTS bronze")
    spark.sql("CREATE SCHEMA IF NOT EXISTS silver")
    for resource in RESOURCES:
        files = sorted(folder.glob(f"{resource}-*.json"))
        assert len(files) == 2, (resource, files)
        pages = [p for file in files for p in json.loads(file.read_text())]
        for p in pages:
            for column in ("extraction_timestamp", "raw_saved_timestamp"):
                p[column] = datetime.fromisoformat(p[column])
        rows = bronze_frame(spark, folder / "raw", pages)
        table = table_name("bronze", resource)
        merge_append(spark, table, BRONZE_SCHEMA, rows, ["event_id"])
        bronze = spark.table(table)
        silver_table = table_name("silver", resource)
        model_silver(silver_history(spark, bronze), resource, "https://hapi.fhir.org/baseR4").write.format(
            "delta").mode("overwrite").saveAsTable(silver_table)
        silver = spark.table(silver_table)
        from pyspark.sql import functions as F
        current_counts = silver.groupBy("resource_id").agg(F.sum(F.col("is_current").cast("int")).alias("current"))
        assert not current_counts.where("current <> 1").take(1)
        print(resource, len(pages), sum(page["match_count"] for page in pages), bronze.count(), silver.count(), flush=True)
    build_gold(spark)
    assert spark.table("gold.fhir_resource_snapshot").count() == len(RESOURCES)
    print("PASS: Gold snapshot has all four resource types", flush=True)


if __name__ == "__main__":
    with TemporaryDirectory() as tmp:
        root = Path(tmp)
        spark = (SparkSession.builder.master("local[2]").appName("FHIR assignment validation")
                 .config("spark.jars", os.environ["DELTA_JARS"])
                 .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                 .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                 .config("spark.sql.warehouse.dir", str(root / "warehouse"))
                 .config("spark.sql.shuffle.partitions", "4")
                 .config("spark.databricks.delta.snapshotPartitions", "2")
                 .config("spark.sql.session.timeZone", "UTC").getOrCreate())
        spark.sparkContext.setLogLevel("WARN")
        try:
            if len(sys.argv) > 1:
                live(spark, Path(sys.argv[1]).resolve())
            else:
                main(spark, root / "raw")
        finally:
            spark.stop()
