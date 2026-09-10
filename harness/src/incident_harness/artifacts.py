import hashlib
import json
import os
import re
from datetime import UTC, datetime
from pathlib import Path


def write_json(path: Path, value) -> None:
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, indent=2) + "\n")
    os.replace(temporary, path)


def phase(run_directory: Path, name: str) -> None:
    with (run_directory / "events.jsonl").open("a") as events:
        events.write(json.dumps({"phase": name, "occurred_at": datetime.now(UTC).isoformat()}) + "\n")


def finalize_success(run_directory: Path, mode: str) -> None:
    phase(run_directory, "finalized")
    write_json(run_directory / "verdict.json", {
        "schema_version": 1,
        "outcome": "resolved",
        "mode": mode,
        "report_status": "not_requested",
        "experiment_complete": False,
        "reason": "Repair verified; model-authored post-mortem is not implemented yet.",
    })
    entries = []
    for path in sorted(run_directory.rglob("*")):
        if not path.is_file() or path.name == "manifest.json" or "bin" in path.parts or "obj" in path.parts:
            continue
        content = path.read_bytes()
        entries.append({
            "path": path.relative_to(run_directory).as_posix(),
            "size": len(content),
            "sha256": hashlib.sha256(content).hexdigest(),
        })
    write_json(run_directory / "manifest.json", {"schema_version": 1, "artifacts": entries})


def inspect_run(root: Path, run_id: str) -> int:
    if not re.fullmatch(r"\d{8}T\d{6}Z-[a-f0-9]{8}", run_id):
        raise ValueError("Invalid run ID")
    verdict = root / "runs" / run_id / "verdict.json"
    if not verdict.is_file():
        raise FileNotFoundError(f"No finalized verdict for {run_id}")
    print(verdict.read_text(), end="")
    return 0
