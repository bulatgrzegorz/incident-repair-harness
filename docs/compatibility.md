# Compatibility Status

Checked on 2026-09-09 on the current macOS development machine.

| Tool | Observed | Status |
| --- | --- | --- |
| .NET SDK | 10.0.103 | Worker build and smoke checks pass. |
| uv | 0.7.19 | Ready for the Python CLI foundation. |
| OpenCode | 1.17.18 | Restricted container startup, NDJSON session capture, and proxy routing passed. |
| Podman client/server | 5.4.1 / 5.4.2, Linux ARM64 VM | Kafka smoke passed. |
| Docker Compose provider | 2.38.1 through `podman compose` | Kafka lifecycle and health wait passed. |
| Apache Kafka | 4.0.0, `sha256:3f7b939115cd4872e9cee9369d80bd69712fde55f9902f46d793f64848dedc75` | Host produce, .NET consume/commit/seek, admin offset read, and restart retry passed. |
| Confluent.Kafka (.NET/Python) | 2.15.0 / 2.15.0 | ARM64 native clients loaded and completed the incident smoke. |
| Grafana LGTM | 0.11.10, `sha256:09d8c3ce4f3a5f2f5b1ef7a3cc526a7e03b608aeef3ea1c67c912df43b2690c9` | Provisioned Grafana 12.1.1 alert, OTLP HTTP metrics/logs, Prometheus queries, and instance labels passed. |
| OpenTelemetry .NET | 1.18.0 | One-second metric export and structured failure-log export passed. |
| TUnit / Microsoft.Testing.Platform | 1.66.27 / 2.4.0 | Pinned `dotnet run` command emitted deterministic TRX in local and external Linux ARM64 modes. |
| Testcontainers | 4.15.0 | A shared TUnit `ClassDataSource` started Apache Kafka and Aspire through the Podman Docker API. |
| Aspire Dashboard | 13.5.2, `sha256:edc005dad8b5426cc06bbde219fc26acaefdd4eb81ea3cf1b5f5209d52e9ac07` | Local and external containers became healthy; test and worker trace exporters target their OTLP endpoints. |
| HTTPX | 0.28.1 | Local Prometheus/Loki polling with inherited proxies disabled passed. |
| OpenCode coding image | 1.17.18 on .NET SDK 10.0.103 and Node 22.19.0 | Non-root/read-only startup and offline cached restore/test passed. |
| Squid | 6.13 on Ubuntu 25.04 | Direct egress denied, OpenAI HTTPS allowed through proxy, and non-allowlisted HTTPS denied. |

Cold LGTM ingestion on Podman was observed 10-12 seconds behind the worker source clock, so the detector freshness bound is 15 seconds on this machine. Samples must still advance throughout the window; a 20-second stale sample remains a failing control.

Podman 5.4.2 accepts loopback TCP connections to ports published from an `internal` bridge but does not forward Kafka protocol responses. The live incident Compose network therefore uses an ordinary bridge with loopback-only published ports. The coding-agent and functional-test networks remain separate and internal.

Restricted containers run with the invoking non-root UID/GID for writable bind mounts; rootful Podman additionally requires `--userns=keep-id`. The Podman path and separate internal candidate network passed in the deterministic fixture at `runs/20260910T052013Z-9ef2ab25`.

Run the proven check with:

```bash
uv run --project harness --locked incident-harness smoke
uv run --project harness --locked incident-harness run --agent fixture
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

On this Podman machine, local Testcontainers mode additionally uses the Podman API socket as `DOCKER_HOST` and disables Ryuk. The same scenario passed in external mode inside the pinned SDK image with no Docker socket or client, against the internal `broker:19092` and `aspire:18889` endpoints.

The external-mode proof started from source copies containing no `bin` or `obj`, restored with `--locked-mode` from a local-only `/feed`, wrote packages and all build output outside the read-only source mount, and then ran the command above with `--property:IsolatedBuildRoot=/build`. Baseline, red-control, and repaired-candidate runs used separately recreated Kafka/Aspire environments. The red control failed behaviorally after 15 seconds with one disposition and committed-next offset `1`; the repaired candidate produced three dispositions, committed offset `3`, and passed.

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

Recorded pinned TRX shapes establish `Passed`, `Failed`, `NotExecuted`, and zero-test counters. Zero tests return Microsoft.Testing.Platform exit code `8`; ordinary assertion failure returns `2`. Stable test identity is `TestMethod@className + TestMethod@name`, not the per-run execution ID.

Three fresh smokes fired the Grafana alert with `noDataState: OK`. The deterministic fixture run at `runs/20260909T172731Z-46aaec7e` also passed source copying without `bin`/`obj` or `.DS_Store`, patch application, a red control against the original processor, four missing-type policy variants against the candidate, same-group recovery, and exact fresh-probe verification.

The Podman internal network also failed to resolve `host.containers.internal` and timed out against its gateway while a host port was deliberately listening. Candidate red/green checks and repaired-worker execution passed in separate restricted containers without Docker access or network package restore.

The corrected-prompt `openai/gpt-5.4-mini` run at `runs/20260909T173417Z-1a090489` completed through the allowlisted proxy. The agent received only Grafana's `alert.json` plus source/test locations, diagnosed the null dereference, changed only the two permitted files, passed the red/green and four-variant policy gates, and resolved the preserved incident through the random probe.

Prometheus `timestamp(...)` freshness checks, recovery telemetry/stability, automated Aspire trace ingestion inspection, the complete functional scenario matrix, runtime-only worker packaging, and OpenCode session continuation/post-mortem generation remain untested. Pin or widen nothing until its corresponding spike passes.
