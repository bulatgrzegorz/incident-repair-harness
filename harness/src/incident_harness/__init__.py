import argparse
import shutil
import subprocess
import sys
from pathlib import Path

from .agent import AGENT_IMAGE, PROXY_IMAGE, prepare_agent
from .artifacts import inspect_run
from .fixture import run_fixture_repair, run_incident_smoke


def _version(command: list[str]) -> tuple[bool, str]:
    if shutil.which(command[0]) is None:
        return False, "not found"

    try:
        result = subprocess.run(command, capture_output=True, text=True, timeout=10, check=False)
    except (OSError, subprocess.TimeoutExpired) as error:
        return False, str(error)

    output = (result.stdout or result.stderr).strip()
    return result.returncode == 0, output or f"exit {result.returncode}"


def doctor(agent: str) -> int:
    root = Path(__file__).resolve().parents[3]
    runtime = "docker" if shutil.which("docker") else "podman"
    checks = [
        ("Python 3.13", sys.version_info[:2] == (3, 13), sys.version.split()[0]),
        (".NET 10", *_version(["dotnet", "--version"])),
        ("uv", *_version(["uv", "--version"])),
        ("container runtime", *_version([runtime, "version", "--format", "{{.Server.Version}}"])),
        ("Compose", *_version([runtime, "compose", "version"])),
        ("patch", *_version(["patch", "--version"])),
        ("worker project", (root / "src/ProductWorker/ProductWorker.csproj").is_file(), "present"),
        ("functional test project", (root / "tests/ProductWorker.Tests/ProductWorker.Tests.csproj").is_file(), "present"),
    ]
    if agent == "opencode":
        checks.append(("agent image", *_version([runtime, "image", "exists", AGENT_IMAGE])))
        checks.append(("proxy image", *_version([runtime, "image", "exists", PROXY_IMAGE])))

    failed = False
    for name, available, detail in checks:
        if name == ".NET 10":
            available = available and detail.startswith("10.")
        failed |= not available
        print(f"{'PASS' if available else 'FAIL'}  {name}: {detail}")

    return 1 if failed else 0


def main() -> int:
    parser = argparse.ArgumentParser(prog="incident-harness")
    subcommands = parser.add_subparsers(dest="command", required=True)
    doctor_parser = subcommands.add_parser("doctor", help="check local prerequisites")
    doctor_parser.add_argument("--agent", choices=["fixture", "opencode"], default="fixture")
    smoke_parser = subcommands.add_parser("smoke", help="reproduce the blocked Kafka consumer")
    smoke_parser.add_argument("--keep", action="store_true", help="keep the broker and its volume")
    prepare_parser = subcommands.add_parser("prepare", help="prepare local execution images")
    prepare_parser.add_argument("--agent", choices=["opencode"], required=True)
    inspect_parser = subcommands.add_parser("inspect", help="print a finalized verdict")
    inspect_parser.add_argument("--run", required=True)
    run_parser = subcommands.add_parser("run", help="run a repair experiment")
    run_parser.add_argument("--agent", choices=["fixture", "opencode"], default="fixture")
    run_parser.add_argument("--model")
    run_parser.add_argument("--credential-env")
    run_parser.add_argument("--keep", action="store_true", help="keep the broker and its volume")
    args = parser.parse_args()
    if args.command == "doctor":
        return doctor(args.agent)
    if args.command == "prepare":
        prepare_agent(Path(__file__).resolve().parents[3])
        return 0
    if args.command == "inspect":
        return inspect_run(Path(__file__).resolve().parents[3], args.run)
    if args.command == "smoke":
        return run_incident_smoke(args.keep)
    if args.agent == "opencode" and (not args.model or not args.credential_env):
        parser.error("--model and --credential-env are required for --agent opencode")
    return run_fixture_repair(args.keep, args.agent, args.model, args.credential_env)
