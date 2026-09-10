import json
import os
import re
import shutil
import subprocess
import time
import uuid
from pathlib import Path


AGENT_IMAGE = "localhost/incident-repair-agent:1.17.18"
PROXY_IMAGE = "docker.io/ubuntu/squid:6.13-25.04_beta@sha256:3de2e64f0ca6efdac3e98557607dc0f23050037f3885016d5d5bfcf9950501b8"


def container_runtime() -> str:
    return "docker" if shutil.which("docker") else "podman"


def prepare_agent(root: Path) -> None:
    runtime = container_runtime()
    subprocess.run(
        [runtime, "build", "--file", str(root / "infrastructure/Agent.Dockerfile"), "--tag", AGENT_IMAGE, str(root)],
        check=True,
        timeout=600,
    )
    subprocess.run([runtime, "pull", PROXY_IMAGE], check=True, timeout=300)


def run_opencode(
    root: Path,
    candidate: Path,
    evidence: Path,
    submission: Path,
    artifacts: Path,
    model: str,
    credential_environment: str,
) -> None:
    provider = model.split("/", 1)[0]
    if provider not in {"openai", "anthropic"}:
        raise ValueError("Only openai/* and anthropic/* are allowlisted")
    if not os.environ.get(credential_environment):
        raise ValueError(f"Missing credential environment variable: {credential_environment}")
    if not re.fullmatch(r"[A-Z][A-Z0-9_]*", credential_environment):
        raise ValueError("Invalid credential environment variable name")

    runtime = container_runtime()
    suffix = uuid.uuid4().hex[:8]
    network = f"incident-agent-{suffix}"
    proxy = f"incident-proxy-{suffix}"
    coding = f"incident-coding-{suffix}"
    config = json.dumps({
        "autoupdate": False,
        "share": "disabled",
        "snapshot": False,
        "plugin": [],
        "mcp": {},
        "enabled_providers": [provider],
        "model": model,
        "small_model": model,
        "permission": {
            "webfetch": "deny",
            "websearch": "deny",
            "codesearch": "deny",
            "task": "deny",
            "external_directory": {"/submission/**": "allow"},
        },
    })
    prompt = (root / "agent/repair-prompt.md").read_text()
    submission.mkdir()
    internet_network = "podman" if runtime == "podman" else "bridge"
    container_user = (["--userns=keep-id"] if runtime == "podman" else []) + ["--user", f"{os.getuid()}:{os.getgid()}"]

    try:
        subprocess.run([runtime, "network", "create", "--internal", network], check=True, timeout=30)
        subprocess.run([
            runtime, "run", "--detach", "--name", proxy, "--network", internet_network,
            "--volume", f"{root / 'infrastructure/squid.conf'}:/etc/squid/squid.conf:ro",
            PROXY_IMAGE,
        ], check=True, timeout=30)
        subprocess.run([runtime, "network", "connect", network, proxy], check=True, timeout=30)
        time.sleep(1)

        command = [
            runtime, "run", "--name", coding, "--network", network, "--read-only",
            *container_user,
            "--cap-drop=all", "--security-opt=no-new-privileges", "--pids-limit=256", "--memory=3g",
            "--tmpfs", "/tmp:rw,size=512m", "--tmpfs", "/home/agent:rw,mode=1777,size=512m",
            "--volume", f"{candidate}:/workspace:rw",
            "--volume", f"{evidence}:/workspace/evidence:ro",
            "--volume", f"{submission}:/submission:rw",
            "--env", "HTTP_PROXY=http://incident-proxy-" + suffix + ":3128",
            "--env", "HTTPS_PROXY=http://incident-proxy-" + suffix + ":3128",
            "--env", "NO_PROXY=",
            "--env", f"{credential_environment}",
            "--env", f"OPENCODE_CONFIG_CONTENT={config}",
            AGENT_IMAGE,
            "opencode", "run", "--pure", "--format", "json", "--model", model, "--dir", "/workspace", prompt,
        ]
        with (artifacts / "repair-stdout.ndjson").open("w") as stdout, (artifacts / "repair-stderr.txt").open("w") as stderr:
            result = subprocess.run(command, stdout=stdout, stderr=stderr, check=False, timeout=600)
        if result.returncode != 0:
            raise RuntimeError(f"OpenCode exited with {result.returncode}")
    finally:
        with (artifacts / "proxy.log").open("w") as proxy_log:
            subprocess.run([runtime, "logs", proxy], stdout=proxy_log, stderr=subprocess.STDOUT, check=False)
        subprocess.run([runtime, "rm", "--force", coding], check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        subprocess.run([runtime, "rm", "--force", proxy], check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        subprocess.run([runtime, "network", "rm", network], check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    sessions = set()
    for line in (artifacts / "repair-stdout.ndjson").read_text().splitlines():
        try:
            event = json.loads(line)
        except json.JSONDecodeError:
            continue
        if event.get("sessionID"):
            sessions.add(event["sessionID"])
    if len(sessions) != 1:
        raise RuntimeError(f"Expected one OpenCode session, received {len(sessions)}")
    (artifacts / "session.json").write_text(json.dumps({"session_id": sessions.pop()}, indent=2) + "\n")

    summary_path = submission / "repair-summary.json"
    if not summary_path.is_file():
        raise RuntimeError("Agent did not submit repair-summary.json")
    summary = json.loads(summary_path.read_text())
    if (
        not all(isinstance(summary.get(field), str) for field in ("diagnosis", "regression", "test_result"))
        or not isinstance(summary.get("changed_files"), list)
        or not all(isinstance(path, str) for path in summary["changed_files"])
    ):
        raise ValueError("Invalid repair-summary.json")
