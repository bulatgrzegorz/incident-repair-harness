import unittest
from tempfile import TemporaryDirectory
from pathlib import Path

from incident_harness.trx import parse_trx


FIXTURES = Path(__file__).parent / "fixtures"


class TrxTests(unittest.TestCase):
    def test_parses_pinned_tunit_outcomes_and_stable_identities(self):
        report = parse_trx(FIXTURES / "tunit-shapes.trx")

        self.assertEqual(report["counters"], {
            "total": 3,
            "executed": 2,
            "passed": 1,
            "failed": 1,
            "notExecuted": 1,
        })
        self.assertEqual(report["tests"], [
            {"identity": "ProductWorker.Tests.RunnerShapeTests.Passes", "outcome": "Passed"},
            {"identity": "ProductWorker.Tests.RunnerShapeTests.Fails", "outcome": "Failed"},
            {"identity": "ProductWorker.Tests.RunnerShapeTests.Skips", "outcome": "NotExecuted"},
        ])

    def test_parses_zero_test_shape_without_treating_it_as_success(self):
        report = parse_trx(FIXTURES / "tunit-zero.trx")

        self.assertEqual(report["counters"]["total"], 0)
        self.assertEqual(report["tests"], [])

    def test_rejects_duplicate_results_even_when_counts_match(self):
        with TemporaryDirectory() as directory:
            path = Path(directory) / "duplicate.trx"
            path.write_text((FIXTURES / "tunit-shapes.trx").read_text().replace(
                'testId="skip" testName="Skips"',
                'testId="fail" testName="Skips"',
                1,
            ))

            with self.assertRaisesRegex(ValueError, "duplicate results"):
                parse_trx(path)


if __name__ == "__main__":
    unittest.main()
