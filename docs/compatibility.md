# Compatibility Status

Checked on 2026-09-10 on the current macOS development machine.

| Tool | Observed | Status |
| --- | --- | --- |
| .NET SDK | 10.0.103 | Worker build and smoke checks pass. |
| OpenCode | 1.17.18 | Restricted container startup, NDJSON session capture, and proxy routing passed. |
| Podman client/server | 5.4.1 / 5.4.2, Linux ARM64 VM | Kafka smoke passed. |
| Docker Compose provider | 2.38.1 through `podman compose` | Kafka lifecycle and health wait passed. |
| Apache Kafka | 4.0.0, `sha256:3f7b939115cd4872e9cee9369d80bd69712fde55f9902f46d793f64848dedc75` | Host produce, .NET consume/commit/seek, admin offset read, and restart retry passed. |
| Confluent.Kafka .NET | 2.15.0 | ARM64 native client loaded and completed the incident smoke. |
| Grafana LGTM | 0.11.10, `sha256:09d8c3ce4f3a5f2f5b1ef7a3cc526a7e03b608aeef3ea1c67c912df43b2690c9` | Provisioned Grafana 12.1.1 alert, OTLP HTTP metrics/logs, Prometheus queries, and instance labels passed. |
| OpenTelemetry .NET | 1.18.0 | One-second metric export and structured failure-log export passed. |
| TUnit / Microsoft.Testing.Platform | 1.66.27 / 2.4.0 | Pinned `dotnet run` command emitted deterministic TRX in local and external Linux ARM64 modes. |
| Testcontainers | 4.15.0 | A shared TUnit `ClassDataSource` started Apache Kafka and Aspire through the Podman Docker API. |
| Aspire Dashboard | 13.5.2, `sha256:edc005dad8b5426cc06bbde219fc26acaefdd4eb81ea3cf1b5f5209d52e9ac07` | Local and external containers became healthy; test and worker trace exporters target their OTLP endpoints. |
| System.Net.Http | .NET 10 | Grafana alert polling with inherited proxies disabled passed. |
| OpenCode coding image | 1.17.18 on .NET SDK 10.0.103 and Node 22.19.0 | Non-root/read-only startup and offline cached restore/test passed. |
| Squid | 6.13 on Ubuntu 25.04 | Direct egress denied, OpenAI HTTPS allowed through proxy, and non-allowlisted HTTPS denied. |

Cold LGTM ingestion on Podman was observed 10-12 seconds behind the worker source clock. The current detector waits for the provisioned alert by name; source timestamp and advancing-sample checks remain planned.

Podman 5.4.2 accepts loopback TCP connections to ports published from an `internal` bridge but does not forward Kafka protocol responses. The live incident Compose network therefore uses an ordinary bridge with loopback-only published ports. The coding-agent and external functional-test networks remain separate and internal.

Restricted containers run with the invoking non-root UID/GID for writable bind mounts; rootful Podman additionally requires `--userns=keep-id`. The Podman path and separate internal candidate network passed in a local deterministic fixture run.

Run the proven check with:

```bash
dotnet run --project harness/dotnet/IncidentHarness.csproj -- prepare --agent fixture
dotnet run --project harness/dotnet/IncidentHarness.csproj -- smoke
dotnet run --project harness/dotnet/IncidentHarness.csproj -- run --agent fixture
```

The pinned functional-test invocation is:

```bash
export DOCKER_HOST="unix://$(podman machine inspect --format '{{.ConnectionInfo.PodmanSocket.Path}}')"
export TESTCONTAINERS_RYUK_DISABLED=true
dotnet restore tests/ProductWorker.Tests/ProductWorker.Tests.csproj --locked-mode
dotnet run --project tests/ProductWorker.Tests/ProductWorker.Tests.csproj \
  --configuration Release --no-restore -- \
  --report-trx \
  --report-trx-filename local.trx \
  --results-directory TestResults/local
```

On this Podman machine, local Testcontainers mode additionally uses the Podman API socket as `DOCKER_HOST` and disables Ryuk. A manual external-mode spike also passed inside the pinned SDK image with no Docker socket or client, against the internal `broker:19092` and `aspire:18889` endpoints.

The main harness now uses the same external test mode against the experiment's existing Kafka and LGTM services to run the agent-authored regression as both a red control and candidate check.

The exact commands inside the no-Docker external SDK container are:

```bash
dotnet restore tests/ProductWorker.Tests/ProductWorker.Tests.csproj \
  --locked-mode --configfile /workspace/infrastructure/NuGet.Config \
  --property:IsolatedBuildRoot=/build
dotnet run --project tests/ProductWorker.Tests/ProductWorker.Tests.csproj \
  --configuration Release --no-restore \
  --property:IsolatedBuildRoot=/build -- \
  --report-trx --report-trx-filename candidate.trx \
  --results-directory /results
```

The container mounts the clean source at `/workspace:ro`, the prepared feed at `/feed:ro`, and fresh writable directories at `/packages`, `/build`, and `/results`. It joins only the network created from `infrastructure/functional-compose.yaml` and receives `FUNCTIONAL_TESTS_MODE=External`, `FUNCTIONAL_TESTS_KAFKA_ENDPOINT=broker:19092`, and `FUNCTIONAL_TESTS_OTLP_ENDPOINT=http://aspire:18889`.

Three fresh smokes fired the Grafana alert with `noDataState: OK`. A local deterministic fixture run also passed source copying without `bin`/`obj` or `.DS_Store`, patch application, a red control against the original processor, four missing-type policy variants against the candidate, same-group recovery, and exact fresh-probe verification.

The .NET 10 harness completed the deterministic fixture run on Podman, including Grafana alert detection, candidate isolation, preserved-state replacement, and partition-drain verification. Its OpenCode mode remains to be validated independently.

The Podman internal network also failed to resolve `host.containers.internal` and timed out against its gateway while a host port was deliberately listening. Candidate red/green checks and repaired-worker execution passed in separate restricted containers without Docker access or network package restore.

A local corrected-prompt `openai/gpt-5.4-mini` run completed through the allowlisted proxy. The agent received only Grafana's `alert.json` plus source/test locations, diagnosed the null dereference, changed only the two permitted files, passed the red/green and four-variant policy gates, and resolved the preserved incident through the random probe.

Prometheus `timestamp(...)` freshness checks, recovery telemetry/stability, automated Aspire trace ingestion inspection, the complete functional scenario matrix, runtime-only worker packaging, and live OpenCode session continuation/post-mortem generation remain untested. Pin or widen nothing until its corresponding spike passes.
