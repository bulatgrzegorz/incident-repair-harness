using System.Diagnostics;

namespace IncidentHarness;

public sealed record ExperimentOptions(
    string Root,
    bool Keep,
    string? Agent,
    string? Model,
    string? CredentialEnvironment);

public static class Experiment
{
    public static async Task<int> Run(ExperimentOptions options, CancellationToken cancellationToken = default)
    {
        var (root, keep, agent, model, credentialEnvironment) = options;

        UnixHost.EnsureSupported();

        var runtime = await ContainerRuntime.Detect(cancellationToken);
        var suffix = Guid.NewGuid().ToString("N")[..8];
        var runId = $"{DateTimeOffset.UtcNow:yyyyMMdd'T'HHmmss'Z'}-{suffix}";
        var project = $"incident-repair-{suffix}";
        var topic = $"products-{suffix}";
        var group = $"product-worker-{suffix}";
        var runDirectory = Path.Combine(root, "runs", runId);
        var outputDirectory = Path.Combine(runDirectory, "output");

        Directory.CreateDirectory(outputDirectory);

        Artifacts.WriteJson(Path.Combine(runDirectory, "run.json"), new
        {
            schema_version = 1,
            run_id = runId,
            started_at = DateTimeOffset.UtcNow.ToString("O"),
            mode = agent ?? "smoke",
            model,
            compose_project = project,
            topic,
            group,
            container_runtime = runtime.Executable,
        });

        Artifacts.Phase(runDirectory, "preflight");

        var totalSteps = agent == "opencode" ? 9 : agent is null ? 4 : 8;
        var currentStep = 0;
        var runTimer = Stopwatch.StartNew();
        var stepTimer = Stopwatch.StartNew();

        void Progress(string message)
        {
            if (currentStep > 0)
            {
                ConsoleUi.StepDone(stepTimer.Elapsed);
            }
            currentStep++;
            stepTimer.Restart();
            ConsoleUi.Step(currentStep, totalSteps, message);
        }

        ConsoleUi.Header(runId, agent ?? "smoke", runtime.Executable);

        var commandLog = Path.Combine(runDirectory, "commands.log");
        var stdoutLog = Path.Combine(runDirectory, "worker.stdout.log");
        var stderrLog = Path.Combine(runDirectory, "worker.stderr.log");

        File.WriteAllText(commandLog, "");
        File.WriteAllText(stdoutLog, "");
        File.WriteAllText(stderrLog, "");

        WorkerProcess? worker = null;
        string? candidateContainer = null;
        KafkaScenario? kafka = null;

        try
        {
            Progress("Starting Kafka and observability services");
            await runtime.StartInfrastructure(root, project, commandLog, cancellationToken);
            Artifacts.Phase(runDirectory, "baseline");

            kafka = new KafkaScenario(topic, group);
            await kafka.CreateTopics();

            Progress("Building the intentionally broken worker");
            var workerDll = await BuildBrokenWorker(root, commandLog, cancellationToken);

            var environment = CreateWorkerEnvironment(topic, group, outputDirectory, runId);
            worker = WorkerProcess.Start(workerDll, environment, stdoutLog, stderrLog);

            Progress("Reproducing the blocked-consumer incident");
            var ledgerPath = Path.Combine(outputDirectory, "processed-products.json");
            var (baseline, poison, tail) = await kafka.InjectIncident(runDirectory, ledgerPath, stderrLog, cancellationToken);

            var attemptsBeforeRestart = CountFailures(stderrLog);
            await worker.DisposeAsync();
            worker = null;
            environment["SERVICE_INSTANCE_ID"] = $"broken-{Guid.NewGuid():N}";
            worker = WorkerProcess.Start(workerDll, environment, stdoutLog, stderrLog);
            Progress("Confirming retries, restart persistence, and Grafana alert");
            await Wait("a retry after worker restart", () => Task.FromResult(!worker.HasExited && CountFailures(stderrLog) > attemptsBeforeRestart), TimeSpan.FromSeconds(15), cancellationToken);
            var alert = await GrafanaEvidence.WaitForAlert(Path.Combine(runDirectory, "telemetry"), cancellationToken: cancellationToken);
            Artifacts.Phase(runDirectory, "detected");
            var committed = await kafka.CommittedOffset();
            if (committed != poison.Offset || LedgerDocument.Read(ledgerPath).Records.Count != 1)
            {
                throw new InvalidOperationException("Restart changed the blocked offset or durable output");
            }

            Artifacts.WriteJson(Path.Combine(runDirectory, "incident-inputs.json"), new[] { baseline, poison, tail });
            var attempts = CountFailures(stderrLog);
            Artifacts.WriteJson(Path.Combine(runDirectory, "incident.json"), new
            {
                schema_version = 1,
                status = "blocked",
                topic,
                group,
                poison_offset = poison.Offset,
                committed_next_offset = committed,
                failure_attempts = attempts,
                restart_confirmed = true,
                grafana_alert = alert,
            });
            ConsoleUi.Insight("Incident confirmed", $"offset {poison.Offset} blocked after {attempts} retries and a worker restart");

            if (agent is null)
            {
                ConsoleUi.StepDone(stepTimer.Elapsed);
                ConsoleUi.Success("INCIDENT REPRODUCED", $"Consumer remains blocked at offset {poison.Offset}.", runDirectory);
                return 0;
            }

            await worker.DisposeAsync();
            worker = null;
            Artifacts.Phase(runDirectory, "repairing");
            Progress($"Preparing the {agent} repair");
            var candidate = await CandidateWorkspace.Prepare(root, runDirectory, agent, model, credentialEnvironment, commandLog, cancellationToken);
            Artifacts.Phase(runDirectory, "frozen");

            Progress("Running functional red/green and malformed-input checks");
            var runtimeCandidate = await CandidateWorkspace.Test(
                runtime, root, candidate, runDirectory, $"{project}_application", cancellationToken);
            Artifacts.Phase(runDirectory, "tested");
            ConsoleUi.Insight("Repair accepted", "functional regression went red to green; 4 malformed-input policies passed");

            Progress("Replacing the worker and verifying recovery");
            environment["SERVICE_INSTANCE_ID"] = $"candidate-{Guid.NewGuid():N}";
            candidateContainer = $"incident-candidate-{suffix}";
            await runtime.RunIncidentCandidate(
                candidateContainer,
                project,
                runtimeCandidate,
                outputDirectory,
                topic,
                group,
                runId,
                environment["SERVICE_INSTANCE_ID"],
                commandLog,
                cancellationToken);
            Artifacts.Phase(runDirectory, "replaced");
            var (probe, checks) = await kafka.VerifyRecovery(ledgerPath, baseline, poison, tail, cancellationToken);
            Artifacts.Phase(runDirectory, "verified");
            ConsoleUi.Insight("Recovery confirmed", $"4 records settled and partition advanced to offset {probe.Offset + 1}");

            Progress("Finalizing evidence");
            Artifacts.WriteJson(Path.Combine(runDirectory, "verification-inputs.json"), new[] { baseline, poison, tail, probe });
            Artifacts.WriteJson(Path.Combine(runDirectory, "verification.json"), new
            {
                schema_version = 1,
                outcome = "resolved",
                mode = agent,
                checks,
            });
            Artifacts.WriteJson(Path.Combine(runDirectory, "test-results.json"), new
            {
                schema_version = 1,
                red_control = "failed_as_expected",
                candidate = "passed",
                policy_variants = new[] { "missing", "null", "empty", "whitespace" },
            });
            await runtime.CaptureLogs(candidateContainer, Path.Combine(runDirectory, "candidate-worker.log"), cancellationToken);
            if (agent == "opencode")
            {
                Progress("Generating the post-mortem from verified evidence");
                await Agent.WritePostMortem(
                    root, candidate, runDirectory, model ?? "", credentialEnvironment ?? "", cancellationToken);
                Artifacts.Phase(runDirectory, "reported");
            }
            Artifacts.FinalizeSuccess(runDirectory, agent);
            ConsoleUi.StepDone(stepTimer.Elapsed);
            ConsoleUi.Success(
                "INCIDENT RESOLVED",
                $"Poison rejected | tail processed | partition drained | {runTimer.Elapsed.TotalSeconds:F1}s",
                runDirectory);
            return 0;
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            ConsoleUi.Error($"RUN CANCELLED  Artifacts: {runDirectory}");
            return 130;
        }
        catch (Exception exception)
        {
            ConsoleUi.Error($"RUN FAILED  {exception.Message}");
            ConsoleUi.Error($"Artifacts: {runDirectory}");
            return 1;
        }
        finally
        {
            if (agent == "opencode")
            {
                await BestEffort(() =>
                {
                    var state = Path.Combine(runDirectory, "agent", "opencode-state");
                    if (Directory.Exists(state))
                    {
                        Directory.Delete(state, recursive: true);
                    }
                    return Task.CompletedTask;
                });
            }
            if (worker is not null)
            {
                await BestEffort(async () => await worker.DisposeAsync());
            }
            if (candidateContainer is not null)
            {
                await BestEffort(() => runtime.RemoveContainer(candidateContainer));
            }
            kafka?.Dispose();
            if (!keep)
            {
                await BestEffort(() => runtime.StopInfrastructure(root, project, commandLog));
            }
        }
    }

    private static Dictionary<string, string> CreateWorkerEnvironment(
        string topic,
        string group,
        string outputDirectory,
        string runId) =>
        new(StringComparer.Ordinal)
        {
            ["KAFKA_BOOTSTRAP_SERVERS"] = "127.0.0.1:9092",
            ["KAFKA_TOPIC"] = topic,
            ["KAFKA_GROUP_ID"] = group,
            ["OUTPUT_DIRECTORY"] = outputDirectory,
            ["SERVICE_NAMESPACE"] = runId,
            ["SERVICE_INSTANCE_ID"] = $"broken-{Guid.NewGuid():N}",
            ["OTEL_EXPORTER_OTLP_ENDPOINT"] = "http://127.0.0.1:4318",
            ["OTEL_EXPORTER_OTLP_PROTOCOL"] = "http/protobuf",
            ["OTEL_METRIC_EXPORT_INTERVAL"] = "1000",
        };

    private static async Task<string> BuildBrokenWorker(string root, string commandLog, CancellationToken cancellationToken)
    {
        var project = Path.Combine(root, "src/ProductWorker/ProductWorker.csproj");
        await ProcessRunner.Run("dotnet", ["restore", project, "--locked-mode"], TimeSpan.FromSeconds(120), logPath: commandLog, cancellationToken: cancellationToken);
        await ProcessRunner.Run("dotnet", ["build", project, "--configuration", "Release", "--no-restore"], TimeSpan.FromSeconds(120), logPath: commandLog, cancellationToken: cancellationToken);
        return Path.Combine(root, "src/ProductWorker/bin/Release/net10.0/ProductWorker.dll");
    }

    private static int CountFailures(string path) => File.ReadAllText(path).Split("product.processing.failed").Length - 1;

    private static async Task Wait(
        string description,
        Func<Task<bool>> condition,
        TimeSpan? timeout = null,
        CancellationToken cancellationToken = default)
    {
        var timer = Stopwatch.StartNew();
        var limit = timeout ?? TimeSpan.FromSeconds(30);
        while (timer.Elapsed < limit)
        {
            if (await condition())
            {
                return;
            }
            await Task.Delay(TimeSpan.FromMilliseconds(250), cancellationToken);
        }
        throw new TimeoutException($"Timed out waiting for {description}");
    }

    private static async Task BestEffort(Func<Task> action)
    {
        try
        {
            await action();
        }
        catch
        {
        }
    }
}
