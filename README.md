# Incident Repair Harness

Minimal foundation for the plan in [`_planning/incident-repair-harness`](../_planning/incident-repair-harness/).

## What works

- .NET 10 Kafka worker with explicit commits and poison-record seeks.
- Atomically replaced, replay-aware disposition ledger.
- Product processing contract for valid and unsupported product types.
- The intentional missing-product-type defect used by the future incident.
- A TUnit functional scenario that runs through real Kafka, the hosted worker, the ledger, and independent offset inspection.
- Local Testcontainers and external harness-provisioned Kafka/Aspire modes using the same scenario.
- A Python `doctor` command and deterministic Podman/Docker incident smoke.
- A deterministic known-good repair with frozen source manifest, red/green policy check, preserved state, and a post-freeze random probe.
- OpenTelemetry metrics/logs exported to pinned Grafana LGTM and a provisioned Grafana blocked-consumer alert.
- Restricted OpenCode 1.17.18 coding image, provider-only Squid egress, two-file source allowlist, and isolated candidate test/deployment containers.

```bash
dotnet run --project tests/ProductWorker.Smoke
dotnet restore tests/ProductWorker.Tests/ProductWorker.Tests.csproj --locked-mode
dotnet run --project tests/ProductWorker.Tests --configuration Release --no-restore -- \
  --report-trx --report-trx-filename local.trx --results-directory TestResults/local
uv run --project harness incident-harness doctor
uv run --project harness incident-harness smoke
uv run --project harness incident-harness run --agent fixture
uv run --project harness incident-harness prepare --agent opencode
```

Process one payload directly:

```bash
dotnet run --project src/ProductWorker -- \
  '{"productId":"P-1","productType":"Physical","price":100}'
```

## Deliberate gaps

This is a foundation, not a pretend-complete harness. The following plan slices remain:

- Trace export, Prometheus source-sample timestamps, telemetry recovery/stability checks, and adversarial stale/missing-oracle controls.
- The remaining worker functional scenario matrix, trusted four-variant policy suite, and automated Aspire ingestion check.
- Runtime-only worker image packaging. The repaired worker currently runs from frozen build output in the restricted SDK image.
- Full strict-ledger schema validation and crash-between-persist-and-commit injection.
- Hard source-size policy beyond the current 1 MB/file ceiling, immutable image identity, stability window, and complete artifact manifest/verdict state machine.
- OpenCode session continuation and model-authored post-mortem generation.

The commands currently own fixed loopback ports `3000`, `3100`, `4318`, `9090`, and `9092`, reject no concurrent run explicitly, and remove state unless `--keep` is supplied.

For a paid OpenCode run, set a dedicated low-budget provider key and name its environment variable explicitly:

```bash
export OPENAI_API_KEY=...
uv run --project harness --locked incident-harness run \
  --agent opencode \
  --model openai/<model> \
  --credential-env OPENAI_API_KEY
```

`anthropic/<model>` with `ANTHROPIC_API_KEY` is also allowlisted. The key is passed by environment name, not placed in command arguments or artifacts. A corrected-prompt `openai/gpt-5.4-mini` run resolved the incident at `runs/20260909T173417Z-1a090489`; its agent evidence view contained only Grafana's `alert.json`.

Add each piece only when implementing its vertical slice. Do not scaffold agent frameworks, deployment abstractions, or dashboards ahead of a working deterministic repair.
