import os
import tempfile
import unittest
from pathlib import Path

from incident_harness.fixture import _clean_build_outputs, _manifest


class CandidateFileTests(unittest.TestCase):
    def test_rejects_links_and_removes_build_outputs(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = root / "source.cs"
            source.write_text("source")
            (root / "bin").mkdir()
            (root / "bin/output.dll").write_text("compiled")

            _clean_build_outputs(root)
            self.assertFalse((root / "bin").exists())

            os.symlink(source, root / "linked.cs")
            with self.assertRaises(ValueError):
                _manifest(root)

            (root / "linked.cs").unlink()
            os.link(source, root / "hard-linked.cs")
            with self.assertRaises(ValueError):
                _manifest(root)


if __name__ == "__main__":
    unittest.main()
