"""Render a Fabric Create Data Pipeline request body for an existing workspace."""
import argparse
from base64 import b64encode
import json
from pathlib import Path
from uuid import UUID

parser = argparse.ArgumentParser()
parser.add_argument("--workspace", required=True, type=UUID)
parser.add_argument("--ingest", required=True, type=UUID)
parser.add_argument("--silver", required=True, type=UUID)
parser.add_argument("--gold", required=True, type=UUID)
args = parser.parse_args()

pipeline = Path(__file__).with_name("pipeline-content.json").read_text()
for placeholder, value in {
    "<WORKSPACE_ID>": args.workspace,
    "<INGEST_NOTEBOOK_ID>": args.ingest,
    "<SILVER_NOTEBOOK_ID>": args.silver,
    "<GOLD_NOTEBOOK_ID>": args.gold,
}.items():
    pipeline = pipeline.replace(placeholder, str(value))

print(json.dumps({
    "displayName": "FHIR_Incremental_Load",
    "description": "Ingest FHIR R4 into Raw and Bronze, then build Silver history and a Gold snapshot.",
    "definition": {"parts": [{
        "path": "pipeline-content.json",
        "payload": b64encode(pipeline.encode()).decode(),
        "payloadType": "InlineBase64",
    }]},
}, indent=2))
