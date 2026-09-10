import json
import tempfile
import unittest
from pathlib import Path

from incident_harness.artifacts import finalize_success, inspect_run


class ArtifactTests(unittest.TestCase):
    def test_finalizes_and_rejects_invalid_run_ids(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            run_id = "20260909T120000Z-1234abcd"
            run = root / "runs" / run_id
            run.mkdir(parents=True)
            (run / "evidence.json").write_text("{}\n")

            finalize_success(run, "fixture")

            manifest = json.loads((run / "manifest.json").read_text())
            self.assertIn("verdict.json", {entry["path"] for entry in manifest["artifacts"]})
            self.assertNotIn("manifest.json", {entry["path"] for entry in manifest["artifacts"]})
            self.assertEqual(0, inspect_run(root, run_id))
            with self.assertRaises(ValueError):
                inspect_run(root, "../escape")


if __name__ == "__main__":
    unittest.main()
