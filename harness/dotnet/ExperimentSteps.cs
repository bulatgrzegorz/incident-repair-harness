namespace IncidentHarness;

public sealed record ConfigurationStepOutput(
    ContainerRuntime Runtime,
    string Suffix,
    string RunId,
    string Root,
    string Project,
    string CommandLogPath,
    string RunDirectory,
    string OutputDirectory,
    string Topic,
    string Group,
    string StdoutLog,
    string StderrLog);

public sealed class ConfigurationStep : IStep
{
    public string Name => "Preparing experiment configuration";

    public async Task ExecuteAsync(ExperimentContext context, CancellationToken cancellationToken)
    {
        var (root, _, agent, model, _) = context.ExperimentOptions;
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
        ConsoleUi.Header(runId, agent ?? "smoke", runtime.Executable);

        var commandLog = Path.Combine(runDirectory, "commands.log");
        var stdoutLog = Path.Combine(runDirectory, "worker.stdout.log");
        var stderrLog = Path.Combine(runDirectory, "worker.stderr.log");

        foreach (var path in new[] { commandLog, stdoutLog, stderrLog })
        {
            await File.WriteAllTextAsync(path, "", cancellationToken);
        }

        context.Configuration = new ConfigurationStepOutput(
            runtime, suffix, runId, root, project, commandLog, runDirectory, outputDirectory,
            topic, group, stdoutLog, stderrLog);
    }
}

public sealed class InfrastructureStepOutput(
    KafkaScenario kafka,
    string workerDll,
    Dictionary<string, string> environment,
    string stdoutLog,
    string stderrLog) : IAsyncDisposable
{
    private WorkerProcess? _worker;

    public KafkaScenario Kafka { get; } = kafka;
    public bool WorkerHasExited => _worker?.HasExited ?? true;

    public void StartWorker()
    {
        if (_worker is not null)
        {
            throw new InvalidOperationException("Worker is already running");
        }
        _worker = WorkerProcess.Start(workerDll, environment, stdoutLog, stderrLog);
    }

    public async Task RestartWorkerAsync()
    {
        await StopWorkerAsync();
        environment["SERVICE_INSTANCE_ID"] = $"broken-{Guid.NewGuid():N}";
        StartWorker();
    }

    public async Task StopWorkerAsync()
    {
        if (_worker is null)
        {
            return;
        }
        var worker = _worker;
        _worker = null;
        await worker.DisposeAsync();
    }

    public async ValueTask DisposeAsync()
    {
        try
        {
            await StopWorkerAsync();
        }
        finally
        {
            Kafka.Dispose();
        }
    }
}

public sealed class InfrastructureStep : IStep
{
    private ConfigurationStepOutput? _configuration;
    private InfrastructureStepOutput? _output;
    private bool _keep;

    public string Name => "Starting Kafka, observability services and worker";

    public async Task ExecuteAsync(ExperimentContext context, CancellationToken cancellationToken)
    {
        var configuration = context.Configuration;
        _configuration = configuration;
        _keep = context.ExperimentOptions.Keep;

        var workerDll = await BuildBrokenWorker(configuration.Root, configuration.CommandLogPath, cancellationToken);
        await configuration.Runtime.StartInfrastructure(configuration.Root, configuration.Project, configuration.CommandLogPath, cancellationToken);

        Artifacts.Phase(configuration.RunDirectory, "baseline");

        var kafka = new KafkaScenario(configuration.Topic, configuration.Group);
        var output = new InfrastructureStepOutput(
            kafka,
            workerDll,
            CreateWorkerEnvironment(configuration),
            configuration.StdoutLog,
            configuration.StderrLog);
        _output = output;
        await kafka.CreateTopics();
        output.StartWorker();
        context.Infrastructure = output;
    }

    public async ValueTask DisposeAsync()
    {
        if (_output is not null)
        {
            await Experiment.BestEffort(async () => await _output.DisposeAsync());
        }

        if (!_keep && _configuration is not null)
        {
            await Experiment.BestEffort(() => _configuration.Runtime.StopInfrastructure(
                _configuration.Root, _configuration.Project, _configuration.CommandLogPath));
        }
    }

    private static async Task<string> BuildBrokenWorker(string root, string commandLog, CancellationToken cancellationToken)
    {
        var project = Path.Combine(root, "src/ProductWorker/ProductWorker.csproj");
        await ProcessRunner.Run("dotnet", ["restore", project, "--locked-mode"], TimeSpan.FromSeconds(120), logPath: commandLog, cancellationToken: cancellationToken);
        await ProcessRunner.Run("dotnet", ["build", project, "--configuration", "Release", "--no-restore"], TimeSpan.FromSeconds(120), logPath: commandLog, cancellationToken: cancellationToken);
        return Path.Combine(root, "src/ProductWorker/bin/Release/net10.0/ProductWorker.dll");
    }

    private static Dictionary<string, string> CreateWorkerEnvironment(ConfigurationStepOutput configuration) =>
        new(StringComparer.Ordinal)
        {
            ["KAFKA_BOOTSTRAP_SERVERS"] = KafkaScenario.BootstrapServers,
            ["KAFKA_TOPIC"] = configuration.Topic,
            ["KAFKA_GROUP_ID"] = configuration.Group,
            ["OUTPUT_DIRECTORY"] = configuration.OutputDirectory,
            ["SERVICE_NAMESPACE"] = configuration.RunId,
            ["SERVICE_INSTANCE_ID"] = $"broken-{Guid.NewGuid():N}",
            ["OTEL_EXPORTER_OTLP_ENDPOINT"] = "http://127.0.0.1:4318",
            ["OTEL_EXPORTER_OTLP_PROTOCOL"] = "http/protobuf",
            ["OTEL_METRIC_EXPORT_INTERVAL"] = "1000",
        };
}

public sealed record ExperimentSetupOutput(
    string LedgerPath,
    PublishedRecord Baseline,
    PublishedRecord Poison,
    PublishedRecord Tail);

public sealed class ExperimentSetupStep : IStep
{
    public string Name => "Reproducing the blocked-consumer incident";

    public async Task ExecuteAsync(ExperimentContext context, CancellationToken cancellationToken)
    {
        var configuration = context.Configuration;
        var kafka = context.Infrastructure.Kafka;

        var ledgerPath = Path.Combine(configuration.OutputDirectory, "processed-products.json");

        var (baseline, poison, tail) = await kafka.InjectIncident(
            configuration.RunDirectory, ledgerPath, configuration.StderrLog, cancellationToken);

        context.Incident = new ExperimentSetupOutput(ledgerPath, baseline, poison, tail);
    }
}

public sealed class InitialExperimentConfirmationStep : IStep
{
    public string Name => "Confirming retries, restart persistence, and Grafana alert";

    public async Task ExecuteAsync(ExperimentContext context, CancellationToken cancellationToken)
    {
        var configuration = context.Configuration;
        var infrastructure = context.Infrastructure;
        var incident = context.Incident;

        var attemptsBeforeRestart = KafkaScenario.CountFailures(configuration.StderrLog);

        await infrastructure.RestartWorkerAsync();

        await KafkaScenario.Wait(
            "a retry after worker restart",
            () => Task.FromResult(!infrastructure.WorkerHasExited && KafkaScenario.CountFailures(configuration.StderrLog) > attemptsBeforeRestart),
            TimeSpan.FromSeconds(15),
            cancellationToken);
        var alert = await GrafanaEvidence.WaitForAlert(
            Path.Combine(configuration.RunDirectory, "telemetry"), cancellationToken: cancellationToken);
        Artifacts.Phase(configuration.RunDirectory, "detected");
        var committed = await infrastructure.Kafka.CommittedOffset();
        if (committed != incident.Poison.Offset || LedgerDocument.Read(incident.LedgerPath).Records.Count != 1)
        {
            throw new InvalidOperationException("Restart changed the blocked offset or durable output");
        }

        Artifacts.WriteJson(Path.Combine(configuration.RunDirectory, "incident-inputs.json"),
            new[] { incident.Baseline, incident.Poison, incident.Tail });
        var attempts = KafkaScenario.CountFailures(configuration.StderrLog);
        Artifacts.WriteJson(Path.Combine(configuration.RunDirectory, "incident.json"), new
        {
            schema_version = 1,
            status = "blocked",
            configuration.Topic,
            configuration.Group,
            poison_offset = incident.Poison.Offset,
            committed_next_offset = committed,
            failure_attempts = attempts,
            restart_confirmed = true,
            grafana_alert = alert,
        });
        ConsoleUi.Insight("Incident confirmed", $"offset {incident.Poison.Offset} blocked after {attempts} retries and a worker restart");
    }
}

public sealed record FixStepOutput(string Candidate, string RuntimeCandidate);

public sealed class FixStep : IStep
{
    private string? _opencodeState;

    public string Name => "Preparing and testing the repair";

    public async Task ExecuteAsync(ExperimentContext context, CancellationToken cancellationToken)
    {
        var options = context.ExperimentOptions;
        var configuration = context.Configuration;

        if (options.Agent == "opencode")
        {
            _opencodeState = Path.Combine(configuration.RunDirectory, "agent", "opencode-state");
        }

        var infrastructure = context.Infrastructure;
        await infrastructure.StopWorkerAsync();

        Artifacts.Phase(configuration.RunDirectory, "repairing");

        var candidate = await CandidateWorkspace.Prepare(
            configuration.Root,
            configuration.RunDirectory,
            options.Agent!,
            options.Model,
            options.CredentialEnvironment,
            configuration.CommandLogPath,
            cancellationToken);

        Artifacts.Phase(configuration.RunDirectory, "frozen");

        var runtimeCandidate = await CandidateWorkspace.Test(
            configuration.Runtime,
            configuration.Root,
            candidate,
            configuration.RunDirectory,
            $"{configuration.Project}_application",
            cancellationToken);

        Artifacts.Phase(configuration.RunDirectory, "tested");
        ConsoleUi.Insight("Repair accepted", "functional regression went red to green; 4 malformed-input policies passed");

        context.Fix = new FixStepOutput(candidate, runtimeCandidate);
    }

    public ValueTask DisposeAsync()
    {
        if (_opencodeState is not null && Directory.Exists(_opencodeState))
        {
            Directory.Delete(_opencodeState, recursive: true);
        }
        return ValueTask.CompletedTask;
    }
}

public sealed record RedeployStepOutput(string Container);

public sealed class RedeployStep : IStep
{
    private ContainerRuntime? _runtime;
    private string? _container;

    public string Name => "Replacing the worker";

    public async Task ExecuteAsync(ExperimentContext context, CancellationToken cancellationToken)
    {
        var configuration = context.Configuration;
        var fix = context.Fix;

        var container = $"incident-candidate-{configuration.Suffix}";

        _runtime = configuration.Runtime;
        _container = container;

        var serviceInstanceId = $"candidate-{Guid.NewGuid():N}";
        await configuration.Runtime.RunIncidentCandidate(
            container,
            configuration.Project,
            fix.RuntimeCandidate,
            configuration.OutputDirectory,
            configuration.Topic,
            configuration.Group,
            configuration.RunId,
            serviceInstanceId,
            configuration.CommandLogPath,
            cancellationToken);

        Artifacts.Phase(configuration.RunDirectory, "replaced");

        context.Redeployment = new RedeployStepOutput(container);
    }

    public ValueTask DisposeAsync() =>
        _runtime is not null && _container is not null
            ? new ValueTask(_runtime.RemoveContainer(_container))
            : ValueTask.CompletedTask;
}

public sealed class VerificationStep : IStep
{
    public string Name => "Verifying recovery and finalizing evidence";

    public async Task ExecuteAsync(ExperimentContext context, CancellationToken cancellationToken)
    {
        var options = context.ExperimentOptions;

        var configuration = context.Configuration;
        var infrastructure = context.Infrastructure;
        var incident = context.Incident;
        var redeploy = context.Redeployment;

        var (probe, checks) = await infrastructure.Kafka.VerifyRecovery(
            incident.LedgerPath, incident.Baseline, incident.Poison, incident.Tail, cancellationToken);

        Artifacts.Phase(configuration.RunDirectory, "verified");
        ConsoleUi.Insight("Recovery confirmed", $"4 records settled and partition advanced to offset {probe.Offset + 1}");

        Artifacts.WriteJson(Path.Combine(configuration.RunDirectory, "verification-inputs.json"),
            new[] { incident.Baseline, incident.Poison, incident.Tail, probe });

        Artifacts.WriteJson(Path.Combine(configuration.RunDirectory, "verification.json"), new
        {
            schema_version = 1,
            outcome = "resolved",
            mode = options.Agent,
            checks,
        });

        Artifacts.WriteJson(Path.Combine(configuration.RunDirectory, "test-results.json"), new
        {
            schema_version = 1,
            red_control = "failed_as_expected",
            candidate = "passed",
            policy_variants = new[] { "missing", "null", "empty", "whitespace" },
        });

        await configuration.Runtime.CaptureLogs(
            redeploy.Container, Path.Combine(configuration.RunDirectory, "candidate-worker.log"), cancellationToken);
    }
}

public sealed class PostMortemStep : IStep
{
    public string Name => "Generating the post-mortem from verified evidence";

    public async Task ExecuteAsync(ExperimentContext context, CancellationToken cancellationToken)
    {
        var options = context.ExperimentOptions;
        var configuration = context.Configuration;
        var fix = context.Fix;

        await Agent.WritePostMortem(
            configuration.Root,
            fix.Candidate,
            configuration.RunDirectory,
            options.Model ?? "",
            options.CredentialEnvironment ?? "",
            cancellationToken);

        Artifacts.Phase(configuration.RunDirectory, "reported");
    }
}
