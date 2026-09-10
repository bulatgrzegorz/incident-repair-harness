import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from incident_harness.evidence import ALERT_NAME, _find_alert, wait_for_grafana_alert


class GrafanaAlertTests(unittest.TestCase):
    def test_ignores_no_data_and_selects_the_product_alert(self):
        no_data = {"labels": {"alertname": "DatasourceNoData", "rulename": ALERT_NAME}}
        firing = {"labels": {"alertname": ALERT_NAME}, "annotations": {"metric": "product_processing_failures_total"}}

        self.assertIs(firing, _find_alert([no_data, firing]))
        self.assertIsNone(_find_alert([no_data]))

    def test_timeout_bounds_requests_and_records_each_observation(self):
        class Response:
            status_code = 200

            def raise_for_status(self):
                pass

            def json(self):
                return []

        class Client:
            request_timeout = None

            def __init__(self, **_):
                pass

            def __enter__(self):
                return self

            def __exit__(self, *_):
                pass

            def get(self, *_, timeout, **__):
                Client.request_timeout = timeout
                return Response()

        with TemporaryDirectory() as directory, \
             patch("incident_harness.evidence.httpx.Client", Client), \
             patch("incident_harness.evidence.time.monotonic", side_effect=[0, 0, 1, 1]):
            with self.assertRaises(TimeoutError):
                wait_for_grafana_alert(Path(directory), timeout=1)

            self.assertEqual(1, Client.request_timeout)
            self.assertEqual(1, len((Path(directory) / "alert-observations.jsonl").read_text().splitlines()))


if __name__ == "__main__":
    unittest.main()
