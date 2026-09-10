import hashlib
import difflib
import json
import os
import shutil
import signal
import subprocess
import time
import uuid
from datetime import UTC, datetime
from pathlib import Path

from confluent_kafka import ConsumerGroupTopicPartitions, Producer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic, OffsetSpec

from .evidence import wait_for_grafana_alert
from .agent import AGENT_IMAGE, run_opencode
from .artifacts import finalize_success, phase, write_json


def _wait(description: str, condition, timeout: float = 30) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if condition():
            return
        time.sleep(0.25)
    raise TimeoutError(f"Timed out waiting for {description}")


def _publish(producer: Producer, topic: str, payload: bytes) -> dict:
    delivered = []
    producer.produce(topic, payload, partition=0, on_delivery=lambda error, message: delivered.append((error, message)))
    if producer.flush(10) != 0 or not delivered or delivered[0][0] is not None:
        raise RuntimeError(f"Kafka delivery failed: {delivered[0][0] if delivered else 'timeout'}")
    message = delivered[0][1]
    return {
        "topic": message.topic(),
        "partition": message.partition(),
        "offset": message.offset(),
        "payload_sha256": hashlib.sha256(payload).hexdigest(),
        "payload": payload.decode(),
    }


def _committed_offset(admin: AdminClient, group: str, topic: str) -> int:
    request = ConsumerGroupTopicPartitions(group, [TopicPartition(topic, 0)])
    result = admin.list_consumer_group_offsets([request], request_timeout=5)[group].result(10)
    return result.topic_partitions[0].offset


def _log_end_offset(admin: AdminClient, topic: str) -> int:
    partition = TopicPartition(topic, 0)
    return admin.list_offsets({partition: OffsetSpec.latest()}, request_timeout=5)[partition].result(10).offset


def _stop_worker(worker: subprocess.Popen | None) -> None:
    if worker is None or worker.poll() is not None:
        return
    os.killpg(worker.pid, signal.SIGINT)
    try:
        worker.wait(10)
    except subprocess.TimeoutExpired:
        os.killpg(worker.pid, signal.SIGKILL)
        worker.wait()


def _start_worker(project: Path, environment: dict, stdout_file, stderr_file) -> subprocess.Popen:
    return subprocess.Popen(
        ["dotnet", "run", "--project", str(project), "--configuration", "Release", "--no-build"],
        env=environment,
        stdin=subprocess.DEVNULL,
        stdout=stdout_file,
        stderr=stderr_file,
        start_new_session=True,
    )


def _manifest(directory: Path) -> list[dict]:
    files = []
    for path in sorted(directory.rglob("*")):
        if path.is_symlink():
            raise ValueError(f"Candidate contains a symlink: {path.relative_to(directory)}")
        if not path.is_file() or "bin" in path.parts or "obj" in path.parts:
            continue
        stat = path.stat()
        if stat.st_nlink != 1 or stat.st_size > 1_000_000:
            raise ValueError(f"Candidate contains an unusual file: {path.relative_to(directory)}")
        content = path.read_bytes()
        files.append({
            "path": path.relative_to(directory).as_posix(),
            "size": len(content),
            "sha256": hashlib.sha256(content).hexdigest(),
        })
    return files


def _clean_build_outputs(directory: Path) -> None:
    for path in sorted(directory.rglob("*"), reverse=True):
        if path.is_dir() and path.name in {"bin", "obj"}:
            shutil.rmtree(path)


def _container_dotnet(runtime: str, directory: Path, arguments: list[str], output, check: bool) -> subprocess.CompletedProcess:
    container_user = (["--userns=keep-id"] if runtime == "podman" else []) + ["--user", f"{os.getuid()}:{os.getgid()}"]
    return subprocess.run([
        runtime, "run", "--rm", "--network", "none", "--read-only",
        *container_user,
        "--cap-drop=all", "--security-opt=no-new-privileges", "--pids-limit=256", "--memory=3g",
        "--tmpfs", "/tmp:rw,size=512m", "--tmpfs", "/home/agent:rw,mode=1777,size=256m",
        "--volume", f"{directory}:/workspace:rw",
        AGENT_IMAGE, "dotnet", *arguments,
    ], stdout=output, stderr=subprocess.STDOUT, check=check, timeout=120, text=True)


def _run_fixture(keep: bool, agent: str | None, model: str | None = None, credential_environment: str | None = None) -> int:
    root = Path(__file__).resolve().parents[3]
    runtime = "docker" if shutil.which("docker") else "podman"
    if shutil.which(runtime) is None:
        raise RuntimeError("Docker or Podman is required")
    container_user = (["--userns=keep-id"] if runtime == "podman" else []) + ["--user", f"{os.getuid()}:{os.getgid()}"]

    suffix = uuid.uuid4().hex[:8]
    run_id = f"{datetime.now(UTC):%Y%m%dT%H%M%SZ}-{suffix}"
    project = f"incident-repair-{suffix}"
    topic = f"products-{suffix}"
    group = f"product-worker-{suffix}"
    run_directory = root / "runs" / run_id
    output_directory = run_directory / "output"
    run_directory.mkdir(parents=True)
    output_directory.mkdir()
    write_json(run_directory / "run.json", {
        "schema_version": 1,
        "run_id": run_id,
        "started_at": datetime.now(UTC).isoformat(),
        "mode": agent or "smoke",
        "model": model,
        "compose_project": project,
        "topic": topic,
        "group": group,
        "container_runtime": runtime,
    })
    phase(run_directory, "preflight")

    compose = [runtime, "compose", "-p", project, "-f", str(root / "infrastructure/compose.yaml")]
    worker = None
    candidate_container = None
    stdout_file = (run_directory / "worker.stdout.log").open("w")
    stderr_file = (run_directory / "worker.stderr.log").open("w")
    try:
        subprocess.run([*compose, "up", "-d", "--wait"], check=True, timeout=120)
        phase(run_directory, "baseline")
        kafka_configuration = {"bootstrap.servers": "127.0.0.1:9092", "log_level": 0}
        admin = AdminClient(kafka_configuration)
        admin.create_topics([NewTopic(topic, 1, 1)])[topic].result(30)
        project_path = str(root / "src/ProductWorker/ProductWorker.csproj")
        subprocess.run(["dotnet", "restore", project_path, "--locked-mode"], check=True, timeout=120)
        subprocess.run(["dotnet", "build", project_path, "--configuration", "Release", "--no-restore"], check=True, timeout=120)

        environment = os.environ | {
            "KAFKA_BOOTSTRAP_SERVERS": "127.0.0.1:9092",
            "KAFKA_TOPIC": topic,
            "KAFKA_GROUP_ID": group,
            "OUTPUT_DIRECTORY": str(output_directory),
            "SERVICE_NAMESPACE": run_id,
            "SERVICE_INSTANCE_ID": f"broken-{uuid.uuid4().hex}",
            "OTEL_EXPORTER_OTLP_ENDPOINT": "http://127.0.0.1:4318",
            "OTEL_EXPORTER_OTLP_PROTOCOL": "http/protobuf",
            "OTEL_METRIC_EXPORT_INTERVAL": "1000",
        }
        worker = _start_worker(root / "src/ProductWorker/ProductWorker.csproj", environment, stdout_file, stderr_file)

        producer = Producer(kafka_configuration)
        baseline = _publish(producer, topic, b'{"productId":"P-baseline","productType":"Physical","price":100}')
        ledger_path = output_directory / "processed-products.json"

        def baseline_complete() -> bool:
            if not ledger_path.exists():
                return False
            ledger = json.loads(ledger_path.read_text())
            committed = _committed_offset(admin, group, topic)
            return len(ledger["records"]) == 1 and committed == baseline["offset"] + 1

        _wait("baseline processing and commit", baseline_complete)
        poison = _publish(producer, topic, b'{"productId":"P-poison","productType":null,"price":100}')
        tail = _publish(producer, topic, b'{"productId":"P-tail","productType":"Digital","price":50}')
        phase(run_directory, "injected")

        failure_log = run_directory / "worker.stderr.log"
        _wait("three poison retries", lambda: failure_log.read_text().count("product.processing.failed") >= 3, 15)
        ledger = json.loads(ledger_path.read_text())
        committed = _committed_offset(admin, group, topic)
        if len(ledger["records"]) != 1 or committed != poison["offset"]:
            raise AssertionError(f"Consumer was not blocked at poison offset {poison['offset']}: commit={committed}, ledger={ledger}")

        _stop_worker(worker)
        environment["SERVICE_INSTANCE_ID"] = f"broken-{uuid.uuid4().hex}"
        worker = _start_worker(root / "src/ProductWorker/ProductWorker.csproj", environment, stdout_file, stderr_file)
        alert = wait_for_grafana_alert(run_directory / "telemetry")
        phase(run_directory, "detected")
        committed = _committed_offset(admin, group, topic)
        if committed != poison["offset"] or len(json.loads(ledger_path.read_text())["records"]) != 1:
            raise AssertionError("Restart changed the blocked offset or durable output")

        (run_directory / "inputs.json").write_text(json.dumps([baseline, poison, tail], indent=2) + "\n")
        (run_directory / "incident.json").write_text(json.dumps({
            "schema_version": 1,
            "status": "blocked",
            "topic": topic,
            "group": group,
            "poison_offset": poison["offset"],
            "committed_next_offset": committed,
            "failure_attempts": failure_log.read_text().count("product.processing.failed"),
            "restart_confirmed": True,
            "grafana_alert": alert,
        }, indent=2) + "\n")

        if agent is None:
            print(f"Blocked-consumer incident reproduced: {run_directory}")
            return 0

        _stop_worker(worker)
        worker = None
        phase(run_directory, "repairing")
        candidate = run_directory / "candidate"
        ignored_build_output = shutil.ignore_patterns("bin", "obj", ".DS_Store")
        shutil.copytree(root / "src", candidate / "src", ignore=ignored_build_output)
        shutil.copytree(root / "tests", candidate / "tests", ignore=ignored_build_output)
        shutil.copy2(root / "Directory.Build.props", candidate)
        shutil.copy2(root / "Directory.Packages.props", candidate)
        shutil.copy2(root / "global.json", candidate)
        original_files = {
            path.relative_to(candidate).as_posix(): path.read_text()
            for path in candidate.rglob("*")
            if path.is_file()
        }
        patch = root / "fixtures/known-good.patch"
        if agent == "fixture":
            subprocess.run(["patch", "-p1", "-i", str(patch)], cwd=candidate, check=True, timeout=30)
            shutil.copy2(patch, run_directory / "source.diff")
        else:
            evidence = run_directory / "agent-evidence"
            evidence.mkdir()
            shutil.copy2(run_directory / "telemetry/alert.json", evidence)
            agent_artifacts = run_directory / "agent"
            agent_artifacts.mkdir()
            run_opencode(
                root,
                candidate,
                evidence,
                run_directory / "submission",
                agent_artifacts,
                model or "",
                credential_environment or "",
            )
            _manifest(candidate)
            _clean_build_outputs(candidate)
            current_files = {
                path.relative_to(candidate).as_posix(): path.read_text()
                for path in candidate.rglob("*")
                if path.is_file() and "bin" not in path.parts and "obj" not in path.parts
            }
            changed = {path for path in original_files.keys() | current_files.keys() if original_files.get(path) != current_files.get(path)}
            allowed = {"src/ProductWorker/ProductProcessor.cs", "tests/ProductWorker.Smoke/Program.cs"}
            if changed != allowed:
                raise ValueError(f"Agent changed invalid paths or omitted its regression: {sorted(changed)}")
            diff = []
            for path in sorted(changed):
                diff.extend(difflib.unified_diff(
                    original_files[path].splitlines(keepends=True),
                    current_files[path].splitlines(keepends=True),
                    fromfile=f"a/{path}",
                    tofile=f"b/{path}",
                ))
            (run_directory / "source.diff").write_text("".join(diff))
        (run_directory / "candidate-source-manifest.json").write_text(json.dumps(_manifest(candidate), indent=2) + "\n")
        phase(run_directory, "frozen")

        red_control = run_directory / "red-control"
        shutil.copytree(root / "src", red_control / "src", ignore=ignored_build_output)
        shutil.copytree(candidate / "tests", red_control / "tests", ignore=ignored_build_output)
        shutil.copy2(root / "Directory.Build.props", red_control)
        shutil.copy2(root / "Directory.Packages.props", red_control)
        shutil.copy2(root / "global.json", red_control)
        red_test = red_control / "tests/ProductWorker.Smoke/ProductWorker.Smoke.csproj"
        red_output = run_directory / "red-control.log"
        with red_output.open("w") as output:
            if agent is not None:
                _container_dotnet(runtime, red_control, ["restore", "tests/ProductWorker.Smoke/ProductWorker.Smoke.csproj", "--locked-mode"], output, True)
                red_result = _container_dotnet(runtime, red_control, ["run", "--project", "tests/ProductWorker.Smoke/ProductWorker.Smoke.csproj", "--configuration", "Release", "--no-restore"], output, False)
            else:
                subprocess.run(["dotnet", "restore", str(red_test), "--locked-mode"], stdout=output, stderr=subprocess.STDOUT, check=True, timeout=120)
                red_result = subprocess.run(
                    ["dotnet", "run", "--project", str(red_test), "--configuration", "Release", "--no-restore"],
                    stdout=output,
                    stderr=subprocess.STDOUT,
                    check=False,
                    timeout=120,
                )
        if red_result.returncode == 0 or "NullReferenceException" not in red_output.read_text():
            raise AssertionError("Regression check was not red against the original processor")

        candidate_test = candidate / "tests/ProductWorker.Smoke/ProductWorker.Smoke.csproj"
        test_output = run_directory / "candidate-test.log"
        with test_output.open("w") as output:
            if agent is not None:
                _container_dotnet(runtime, candidate, ["restore", "tests/ProductWorker.Smoke/ProductWorker.Smoke.csproj", "--locked-mode"], output, True)
                _container_dotnet(runtime, candidate, ["run", "--project", "tests/ProductWorker.Smoke/ProductWorker.Smoke.csproj", "--configuration", "Release", "--no-restore"], output, True)
            else:
                subprocess.run(["dotnet", "restore", str(candidate_test), "--locked-mode"], stdout=output, stderr=subprocess.STDOUT, check=True, timeout=120)
                subprocess.run(["dotnet", "run", "--project", str(candidate_test), "--configuration", "Release", "--no-restore"], stdout=output, stderr=subprocess.STDOUT, check=True, timeout=120)

        policy_payloads = [
            '{"productId":"P-policy","price":10}',
            '{"productId":"P-policy","productType":null,"price":10}',
            '{"productId":"P-policy","productType":"","price":10}',
            '{"productId":"P-policy","productType":"  ","price":10}',
        ]
        policy_results = []
        for payload in policy_payloads:
            if agent is not None:
                result = _container_dotnet(
                    runtime,
                    candidate,
                    ["run", "--project", "src/ProductWorker/ProductWorker.csproj", "--configuration", "Release", "--no-build", "--", payload],
                    subprocess.PIPE,
                    True,
                )
            else:
                result = subprocess.run(
                    ["dotnet", "run", "--project", str(candidate / "src/ProductWorker/ProductWorker.csproj"), "--configuration", "Release", "--no-build", "--", payload],
                    capture_output=True,
                    text=True,
                    check=True,
                    timeout=120,
                )
            policy_result = json.loads(result.stdout.splitlines()[-1])
            if policy_result.get("disposition") != "rejected" or policy_result.get("reasonCode") != "missing_product_type":
                raise AssertionError(f"Malformed-type policy failed: {policy_result}")
            policy_results.append(policy_result)
        (run_directory / "policy-results.json").write_text(json.dumps(policy_results, indent=2) + "\n")
        phase(run_directory, "tested")

        candidate_project = candidate / "src/ProductWorker/ProductWorker.csproj"
        environment["SERVICE_INSTANCE_ID"] = f"candidate-{uuid.uuid4().hex}"
        if agent is not None:
            candidate_container = f"incident-candidate-{suffix}"
            subprocess.run([
                runtime, "run", "--detach", "--name", candidate_container, "--network", f"{project}_application", "--read-only",
                *container_user,
                "--cap-drop=all", "--security-opt=no-new-privileges", "--pids-limit=256", "--memory=1g",
                "--tmpfs", "/tmp:rw,size=128m", "--tmpfs", "/home/agent:rw,mode=1777,size=128m",
                "--volume", f"{candidate}:/workspace:ro", "--volume", f"{output_directory}:/output:rw",
                "--env", "KAFKA_BOOTSTRAP_SERVERS=broker:19092", "--env", f"KAFKA_TOPIC={topic}",
                "--env", f"KAFKA_GROUP_ID={group}", "--env", "OUTPUT_DIRECTORY=/output",
                "--env", f"SERVICE_NAMESPACE={run_id}", "--env", f"SERVICE_INSTANCE_ID={environment['SERVICE_INSTANCE_ID']}",
                "--env", "OTEL_EXPORTER_OTLP_ENDPOINT=http://lgtm:4318", "--env", "OTEL_EXPORTER_OTLP_PROTOCOL=http/protobuf",
                AGENT_IMAGE, "dotnet", "run", "--project", "src/ProductWorker/ProductWorker.csproj", "--configuration", "Release", "--no-build",
            ], check=True, timeout=30)
        else:
            worker = _start_worker(candidate_project, environment, stdout_file, stderr_file)
        phase(run_directory, "replaced")

        def recovered_through_tail() -> bool:
            records = json.loads(ledger_path.read_text())["records"]
            return len(records) == 3 and _committed_offset(admin, group, topic) == tail["offset"] + 1

        _wait("poison rejection and tail recovery", recovered_through_tail, 30)
        probe_payload = json.dumps({
            "productId": f"P-probe-{uuid.uuid4().hex}",
            "productType": "Physical",
            "price": 75,
        }, separators=(",", ":")).encode()
        probe = _publish(producer, topic, probe_payload)

        def probe_complete() -> bool:
            records = json.loads(ledger_path.read_text())["records"]
            return len(records) == 4 and _committed_offset(admin, group, topic) == probe["offset"] + 1

        _wait("fresh probe processing", probe_complete, 30)
        records = json.loads(ledger_path.read_text())["records"]
        by_offset = {record["offset"]: record for record in records}
        probe_input = json.loads(probe_payload)
        checks = {
            "baseline_preserved": {
                "payload_sha256": baseline["payload_sha256"],
                "disposition": "processed",
                "product_id": "P-baseline",
                "normalized_type": "physical",
                "price": 100,
            }.items() <= by_offset[baseline["offset"]].items(),
            "poison_rejected": {
                "payload_sha256": poison["payload_sha256"],
                "disposition": "rejected",
                "product_id": "P-poison",
                "reason_code": "missing_product_type",
            }.items() <= by_offset[poison["offset"]].items(),
            "tail_processed": {
                "payload_sha256": tail["payload_sha256"],
                "disposition": "processed",
                "product_id": "P-tail",
                "normalized_type": "digital",
                "price": 50,
            }.items() <= by_offset[tail["offset"]].items(),
            "probe_processed": {
                "payload_sha256": probe["payload_sha256"],
                "disposition": "processed",
                "product_id": probe_input["productId"],
                "normalized_type": "physical",
                "price": 75,
            }.items() <= by_offset[probe["offset"]].items(),
            "no_extra_records": set(by_offset) == {baseline["offset"], poison["offset"], tail["offset"], probe["offset"]},
            "partition_drained": _committed_offset(admin, group, topic) == _log_end_offset(admin, topic) == probe["offset"] + 1,
        }
        if not all(checks.values()):
            raise AssertionError(f"Recovery verification failed: {checks}")
        phase(run_directory, "verified")

        (run_directory / "inputs.json").write_text(json.dumps([baseline, poison, tail, probe], indent=2) + "\n")
        (run_directory / "verification.json").write_text(json.dumps({
            "schema_version": 1,
            "outcome": "resolved",
            "mode": agent,
            "checks": checks,
        }, indent=2) + "\n")
        (run_directory / "test-results.json").write_text(json.dumps({
            "schema_version": 1,
            "red_control": "failed_as_expected",
            "candidate": "passed",
            "policy_variants": ["missing", "null", "empty", "whitespace"],
        }, indent=2) + "\n")
        if candidate_container:
            with (run_directory / "candidate-worker.log").open("w") as output:
                subprocess.run([runtime, "logs", candidate_container], stdout=output, stderr=subprocess.STDOUT, check=False)
        finalize_success(run_directory, agent)
        print(f"{agent.capitalize()} repair resolved the incident: {run_directory}")
        return 0
    finally:
        _stop_worker(worker)
        if candidate_container:
            subprocess.run([runtime, "rm", "--force", candidate_container], check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        stdout_file.close()
        stderr_file.close()
        if not keep:
            subprocess.run([*compose, "down", "--volumes"], check=False, timeout=60)


def run_incident_smoke(keep: bool) -> int:
    return _run_fixture(keep, agent=None)


def run_fixture_repair(keep: bool, agent: str = "fixture", model: str | None = None, credential_environment: str | None = None) -> int:
    return _run_fixture(keep, agent, model, credential_environment)
