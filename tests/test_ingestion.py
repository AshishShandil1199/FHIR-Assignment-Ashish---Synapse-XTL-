import json
import unittest
from datetime import date
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch
from urllib.error import HTTPError, URLError

from fhir_extract import canonical_resource, get_bundle, land_day, search_days


class IngestionTests(unittest.TestCase):
    def test_dates(self):
        days = list(search_days("", "", 2, "2026-11-13T23:01:00+02:00"))
        self.assertEqual(days, [date(2026, 11, 11), date(2026, 11, 12)])
        with self.assertRaises(ValueError):
            list(search_days("2026-11-11", "", 2))

    def test_pagination_and_counts(self):
        patient = {"resourceType": "Patient", "id": "p1"}
        body = json.dumps({
            "resourceType": "Bundle", "type": "searchset",
            "entry": [
                {"resource": patient},
                {"resource": patient, "search": {"mode": "include"}},
                {"resource": {"resourceType": "Observation"}},
            ],
            "link": [{"relation": "next", "url": "?page=2"}],
        }, indent=2).encode()
        empty = b'{"resourceType":"Bundle","type":"searchset","entry":[]}'
        answers = iter([body, empty])
        with TemporaryDirectory() as folder:
            pages = land_day(folder, "https://example.test/fhir", "Patient",
                             date(2026, 11, 11), 200, 2, "test",
                             fetch=lambda url: next(answers))
            self.assertEqual([page["match_count"] for page in pages], [1, 0])
            self.assertTrue(pages[1]["api_url_or_params"].endswith("?page=2"))
            self.assertEqual((Path(folder) / pages[0]["relative_path"]).read_bytes(), body)

    def test_page_limit_and_origin(self):
        for link, error in [("?page=2", RuntimeError),
                            ("https://other.test/Patient", ValueError)]:
            body = json.dumps({
                "resourceType": "Bundle", "type": "searchset",
                "link": [{"relation": "next", "url": link}],
            }).encode()
            limit = 1 if error is RuntimeError else 2
            with TemporaryDirectory() as folder, self.assertRaises(error):
                land_day(folder, "https://example.test/fhir", "Patient",
                         date(2026, 11, 11), 200, limit, "test",
                         fetch=lambda url: body)

    def test_hashes(self):
        patient = {"resourceType": "Patient", "id": "p1", "gender": "female",
                   "meta": {"versionId": "1", "lastUpdated": "2026-11-11T01:00:00Z",
                            "tag": [{"code": "test"}]}}
        original = canonical_resource(json.dumps(patient))
        patient["meta"]["versionId"] = "2"
        updated = canonical_resource(json.dumps(patient))
        self.assertEqual(original[1], updated[1])
        self.assertNotEqual(original[2], updated[2])
        patient["gender"] = "other"
        self.assertNotEqual(updated[1], canonical_resource(json.dumps(patient))[1])
        self.assertEqual(json.loads(updated[0])["meta"]["tag"], [{"code": "test"}])

    def test_retry_after_and_permanent_error(self):
        busy = HTTPError("https://example.test", 429, "busy", {"Retry-After": "90"}, None)
        errors = [busy, URLError("offline"), URLError("offline"), URLError("offline")]
        with patch("fhir_extract.urlopen", side_effect=errors), \
                patch("fhir_extract.time.sleep") as sleep:
            with self.assertRaises(URLError):
                get_bundle("https://example.test")
            self.assertEqual([call.args[0] for call in sleep.call_args_list], [60, 2, 4])

        missing = HTTPError("https://example.test", 404, "missing", {}, None)
        with patch("fhir_extract.urlopen", side_effect=missing) as request, \
                patch("fhir_extract.time.sleep") as sleep:
            with self.assertRaises(HTTPError):
                get_bundle("https://example.test")
            self.assertEqual(request.call_count, 1)
            sleep.assert_not_called()


if __name__ == "__main__":
    unittest.main()
