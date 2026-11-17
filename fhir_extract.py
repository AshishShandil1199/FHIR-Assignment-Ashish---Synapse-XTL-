"""FHIR R4 search pagination and byte-for-byte Raw landing."""
import hashlib
import json
import re
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, urljoin, urlsplit
from urllib.request import Request, urlopen
from uuid import uuid4

RESOURCES = ("Patient", "Encounter", "Observation", "Condition")
DEFAULT_BASE = "https://hapi.fhir.org/baseR4"
FHIR_ID = re.compile(r"^[A-Za-z0-9.-]{1,64}$")


def now():
    return datetime.now(timezone.utc)


def search_days(start, end, backfill_days, anchor_utc=""):
    if anchor_utc:
        anchor = datetime.fromisoformat(anchor_utc.replace("Z", "+00:00"))
    else:
        anchor = now()
    if anchor.tzinfo is None:
        raise ValueError("anchor_utc requires a UTC offset")
    if bool(start) != bool(end):
        raise ValueError("Pass both window dates or leave both blank")
    if start:
        first = date.fromisoformat(start)
        last = date.fromisoformat(end)
    else:
        last = anchor.astimezone(timezone.utc).date()
        first = last - timedelta(days=int(backfill_days))
    if last <= first:
        raise ValueError("The end date must be exclusive and later than the start")
    for day in range((last - first).days):
        yield first + timedelta(days=day)


def get_bundle(url):
    request = Request(url, headers={"Accept": "application/fhir+json"})
    for attempt in range(4):
        try:
            with urlopen(request, timeout=60) as response:
                return response.read()
        except (HTTPError, URLError, TimeoutError) as exc:
            if attempt == 3:
                raise
            retry_after = ""
            if isinstance(exc, HTTPError):
                if exc.code not in (429, 500, 502, 503, 504):
                    raise
                retry_after = exc.headers.get("Retry-After", "")
            if retry_after.isdigit():
                delay = min(int(retry_after), 60)
            else:
                delay = 2 ** attempt
            time.sleep(delay)


def initial_url(base, resource, day, page_size):
    tomorrow = day + timedelta(days=1)
    params = [("_lastUpdated", f"ge{day}T00:00:00Z"),
              ("_lastUpdated", f"lt{tomorrow}T00:00:00Z"),
              ("_count", page_size)]
    return f"{base.rstrip('/')}/{resource}?{urlencode(params)}"


def land_day(raw_root, base, resource, day, page_size, max_pages, run_id, fetch=get_bundle, page_log=None):
    if resource not in RESOURCES or not 1 <= page_size <= 1000 or max_pages < 1:
        raise ValueError("Invalid resource, page size or page limit")
    base_parts = urlsplit(base)
    if base_parts.scheme != "https" or not base_parts.hostname or base_parts.username:
        raise ValueError("Use an HTTPS FHIR base URL without user information")
    if not re.fullmatch(r"[A-Za-z0-9_-]{1,100}", run_id):
        raise ValueError("Invalid run ID")
    attempt_id = uuid4().hex
    folder = Path(raw_root) / resource / f"date={day}" / run_id / attempt_id
    folder.mkdir(parents=True)
    url = initial_url(base, resource, day, page_size)
    seen = set()
    if page_log is None:
        log = []
    else:
        log = page_log
    for page in range(1, max_pages + 1):
        target = urlsplit(url)
        if (target.scheme, target.netloc) != (base_parts.scheme, base_parts.netloc):
            raise ValueError("Pagination link changed origin")
        if url in seen:
            raise ValueError("Pagination repeated a URL")
        seen.add(url)
        requested_at = now()
        body = fetch(url)
        filename = folder / f"page-{page:06d}.json"
        filename.write_bytes(body)
        saved_at = now()
        bundle = json.loads(body)
        if bundle.get("resourceType") != "Bundle" or bundle.get("type") != "searchset":
            raise ValueError(f"Expected a FHIR searchset Bundle for {resource} on {day}")
        matches = 0
        for item in bundle.get("entry", []):
            mode = item.get("search", {}).get("mode", "match")
            entry_type = item.get("resource", {}).get("resourceType")
            if mode == "match" and entry_type == resource:
                matches += 1
        log.append({
            "resource_type": resource, "source_date": str(day), "run_id": run_id,
            "attempt_id": attempt_id, "page_number": page, "relative_path": str(filename.relative_to(raw_root)),
            "extraction_timestamp": requested_at, "raw_saved_timestamp": saved_at,
            "api_url_or_params": url, "match_count": matches
        })
        next_link = None
        for link in bundle.get("link", []):
            if link.get("relation") == "next":
                next_link = link["url"]
                break
        if not next_link:
            return log
        url = urljoin(url, next_link)
    raise RuntimeError(f"Exceeded max_pages for {resource} on {day}; no partial Bronze load")


def canonical_resource(resource_json):
    """Preserve the original canonical JSON and hash contract across replays."""
    obj = json.loads(resource_json)
    payload = json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    meta = obj.get("meta", {})
    business_meta = meta.copy()
    business_meta.pop("versionId", None)
    business_meta.pop("lastUpdated", None)
    if business_meta:
        obj["meta"] = business_meta
    else:
        obj.pop("meta", None)
    business = json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    content_hash = hashlib.sha256(business.encode()).hexdigest()
    event_id = hashlib.sha256(payload.encode()).hexdigest()
    return payload, content_hash, event_id
