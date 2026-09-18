using System.Diagnostics;
using System.Text.Json.Nodes;

namespace IncidentHarness;

public sealed class ExecutionContext(ExperimentOptions experimentOptions)
{
    public ExperimentOptions ExperimentOptions { get; } = experimentOptions;
    public ConfigurationStepOutput? ConfigurationStepOutput { get; private set; }
    public InfrastructureStepOutput? InfrastructureStepOutput { get; private set; }
    public ExperimentSetupOutput? ExperimentSetupOutput { get; private set; }
    public InitialExperimentConfirmationOutput? InitialExperimentConfirmationOutput { get; private set; }
    public FixStepOutput? FixStepOutput { get; private set; }
    public RedeployStepOutput? RedeployStepOutput { get; private set; }
    public VerificationStepOutput? VerificationStepOutput { get; private set; }
    public PostMortemStepOutput? PostMortemStepOutput { get; private set; }

    public void Set<T>(T value)
    {
        var property = GetType().GetProperties().SingleOrDefault(p => p.CanWrite && p.PropertyType == typeof(T));
        if (property is null)
        {
            throw new InvalidOperationException($"No writable property for {typeof(T).Name}");
        }
        property.SetValue(this, value);
    }

    public T Get<T>()
    {
        var property = GetType().GetProperties().SingleOrDefault(p => p.PropertyType == typeof(T));
        return property?.GetValue(this) is not T value ? 
            throw new InvalidOperationException($"No value for {typeof(T).Name}") : 
            value;
    }
    
    public T? TryGet<T>() where T : class =>
        GetType().GetProperties().SingleOrDefault(p => p.PropertyType == typeof(T))?.GetValue(this) as T;
}

public interface IStep : IAsyncDisposable
{
    string Name { get; }
    Task ExecuteAsync(ExecutionContext context, CancellationToken cancellationToken);
    ValueTask IAsyncDisposable.DisposeAsync() => ValueTask.CompletedTask;
}

public sealed class StepExecutionService
{
    public async Task<int> Execute(ExperimentOptions experimentOptions, CancellationToken cancellationToken)
    {
        var context = new ExecutionContext(experimentOptions);
        var runTimer = Stopwatch.StartNew();
        var steps = CreateSteps(experimentOptions.Agent);

        try
        {
            var stepTimer = Stopwatch.StartNew();
            var currentStep = 0;

            foreach (var step in steps)
            {
                ConsoleUi.Step(++currentStep, steps.Length, step.Name);
                stepTimer.Restart();
                
                await step.ExecuteAsync(context, cancellationToken);

                ConsoleUi.StepDone(stepTimer.Elapsed);
            }

            var configuration = context.Get<ConfigurationStepOutput>();
            if (experimentOptions.Agent is null)
            {
                var incident = context.Get<ExperimentSetupOutput>();
                ConsoleUi.Success("INCIDENT REPRODUCED", $"Consumer remains blocked at offset {incident.Poison.Offset}.", configuration.RunDirectory);
                return 0;
            }

            Artifacts.FinalizeSuccess(configuration.RunDirectory, experimentOptions.Agent);
            ConsoleUi.Success(
                "INCIDENT RESOLVED",
                $"Poison rejected | tail processed | partition drained | {runTimer.Elapsed.TotalSeconds:F1}s",
                configuration.RunDirectory);
            return 0;
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            ConsoleUi.Error($"RUN CANCELLED{ArtifactLocation(context)}");
            return 130;
        }
        catch (Exception exception)
        {
            ConsoleUi.Error($"RUN FAILED  {exception.Message}");
            var location = ArtifactLocation(context);
            if (location.Length > 0)
            {
                ConsoleUi.Error(location.TrimStart());
            }
            return 1;
        }
        finally
        {
            foreach (var step in steps.Reverse())
            {
                await BestEffort(async () => await step.DisposeAsync());
            }
        }
    }

    private static IStep[] CreateSteps(string? agent)
    {
        List<IStep> steps =
        [
            new ConfigurationStep(),
            new InfrastructureStep(),
            new ExperimentSetupStep(),
            new InitialExperimentConfirmationStep(),
        ];
        
        if (agent is not null)
        {
            steps.AddRange([new FixStep(), new RedeployStep(), new VerificationStep()]);
        }
        
        if (agent == "opencode")
        {
            steps.Add(new PostMortemStep());
        }
        
        return [.. steps];
    }

    private static string ArtifactLocation(ExecutionContext context) =>
        context.TryGet<ConfigurationStepOutput>() is { } configuration
            ? $"  Artifacts: {configuration.RunDirectory}"
            : "";

    private static async Task BestEffort(Func<Task> action)
    {
        try
        {
            await action();
        }
        catch
        {
            // ignored
        }
    }
}

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

    public async Task ExecuteAsync(ExecutionContext context, CancellationToken cancellationToken)
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
        
        await File.WriteAllTextAsync(commandLog, "", cancellationToken);
        await File.WriteAllTextAsync(stdoutLog, "", cancellationToken);
        await File.WriteAllTextAsync(stderrLog, "", cancellationToken);

        context.Set(new ConfigurationStepOutput(
            runtime, suffix, runId, root, project, commandLog, runDirectory, outputDirectory,
            topic, group, stdoutLog, stderrLog));
    }
}

public sealed class InfrastructureStepOutput
{
    public KafkaScenario? Kafka { get; set; }
    public WorkerProcess? Worker { get; set; }
    public required string WorkerDll { get; init; }
    public required Dictionary<string, string> Environment { get; init; }
}

public sealed class InfrastructureStep : IStep
{
    private ConfigurationStepOutput? _configuration;
    private InfrastructureStepOutput? _output;
    private bool _keep;

    public string Name => "Starting Kafka, observability services and worker";

    public async Task ExecuteAsync(ExecutionContext context, CancellationToken cancellationToken)
    {
        var configuration = context.Get<ConfigurationStepOutput>();
        _configuration = configuration;
        _keep = context.ExperimentOptions.Keep;
        
        var output = new InfrastructureStepOutput
        {
            WorkerDll = await BuildBrokenWorker(configuration.Root, configuration.CommandLogPath, cancellationToken),
            Environment = CreateWorkerEnvironment(configuration),
        };
        
        await configuration.Runtime.StartInfrastructure(configuration.Root, configuration.Project, configuration.CommandLogPath, cancellationToken);
        
        Artifacts.Phase(configuration.RunDirectory, "baseline");
        
        output.Kafka = new KafkaScenario(configuration.Topic, configuration.Group);
        await output.Kafka.CreateTopics();
        
        output.Worker = WorkerProcess.Start(output.WorkerDll, output.Environment, configuration.StdoutLog, configuration.StderrLog);
        
        _output = output;
        context.Set(output);
    }

    public async ValueTask DisposeAsync()
    {
        if (_output?.Worker is not null)
        {
            await BestEffort(async () => await _output.Worker.DisposeAsync());
        }
        
        if (_output?.Kafka is not null)
        {
            await BestEffort(() =>
            {
                _output.Kafka.Dispose();
                return Task.CompletedTask;
            });
        }
        
        if (!_keep && _configuration is not null)
        {
            await BestEffort(() => _configuration.Runtime.StopInfrastructure(
                _configuration.Root, _configuration.Project, _configuration.CommandLogPath));
        }
    }

    private static async Task BestEffort(Func<Task> action)
    {
        try
        {
            await action();
        }
        catch
        {
            // ignored
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

    public async Task ExecuteAsync(ExecutionContext context, CancellationToken cancellationToken)
    {
        var configuration = context.Get<ConfigurationStepOutput>();
        var kafka = context.Get<InfrastructureStepOutput>().Kafka!;
        
        var ledgerPath = Path.Combine(configuration.OutputDirectory, "processed-products.json");
        
        var (baseline, poison, tail) = 
            await kafka.InjectIncident(
            configuration.RunDirectory, ledgerPath, configuration.StderrLog, cancellationToken);
        
        context.Set(new ExperimentSetupOutput(ledgerPath, baseline, poison, tail));
    }
}

public sealed record InitialExperimentConfirmationOutput(long CommittedOffset, int FailureAttempts, JsonNode Alert);

public sealed class InitialExperimentConfirmationStep : IStep
{
    public string Name => "Confirming retries, restart persistence, and Grafana alert";

    public async Task ExecuteAsync(ExecutionContext context, CancellationToken cancellationToken)
    {
        var configuration = context.Get<ConfigurationStepOutput>();
        var infrastructure = context.Get<InfrastructureStepOutput>();
        var incident = context.Get<ExperimentSetupOutput>();
        
        var attemptsBeforeRestart = CountFailures(configuration.StderrLog);
        
        await infrastructure.Worker!.DisposeAsync();
        
        infrastructure.Worker = null;
        infrastructure.Environment["SERVICE_INSTANCE_ID"] = $"broken-{Guid.NewGuid():N}";
        infrastructure.Worker = WorkerProcess.Start(
            infrastructure.WorkerDll, infrastructure.Environment, configuration.StdoutLog, configuration.StderrLog);

        await Wait(
            "a retry after worker restart",
            () => Task.FromResult(!infrastructure.Worker.HasExited && CountFailures(configuration.StderrLog) > attemptsBeforeRestart),
            TimeSpan.FromSeconds(15),
            cancellationToken);
        var alert = await GrafanaEvidence.WaitForAlert(
            Path.Combine(configuration.RunDirectory, "telemetry"), cancellationToken: cancellationToken);
        Artifacts.Phase(configuration.RunDirectory, "detected");
        var committed = await infrastructure.Kafka!.CommittedOffset();
        if (committed != incident.Poison.Offset || LedgerDocument.Read(incident.LedgerPath).Records.Count != 1)
        {
            throw new InvalidOperationException("Restart changed the blocked offset or durable output");
        }

        Artifacts.WriteJson(Path.Combine(configuration.RunDirectory, "incident-inputs.json"),
            new[] { incident.Baseline, incident.Poison, incident.Tail });
        var attempts = CountFailures(configuration.StderrLog);
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
        context.Set(new InitialExperimentConfirmationOutput(committed, attempts, alert));
    }

    private static int CountFailures(string path) => File.ReadAllText(path).Split("product.processing.failed").Length - 1;

    private static async Task Wait(
        string description,
        Func<Task<bool>> condition,
        TimeSpan timeout,
        CancellationToken cancellationToken)
    {
        var timer = Stopwatch.StartNew();
        while (timer.Elapsed < timeout)
        {
            if (await condition())
            {
                return;
            }
            await Task.Delay(TimeSpan.FromMilliseconds(250), cancellationToken);
        }
        throw new TimeoutException($"Timed out waiting for {description}");
    }
}

public sealed record FixStepOutput(string Candidate, string RuntimeCandidate);

public sealed class FixStep : IStep
{
    private string? _opencodeState;

    public string Name => "Preparing and testing the repair";

    public async Task ExecuteAsync(ExecutionContext context, CancellationToken cancellationToken)
    {
        var options = context.ExperimentOptions;
        var configuration = context.Get<ConfigurationStepOutput>();
        
        if (options.Agent == "opencode")
        {
            _opencodeState = Path.Combine(configuration.RunDirectory, "agent", "opencode-state");
        }
        
        var infrastructure = context.Get<InfrastructureStepOutput>();
        await infrastructure.Worker!.DisposeAsync();
        infrastructure.Worker = null;
        
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
        
        context.Set(new FixStepOutput(candidate, runtimeCandidate));
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

    public async Task ExecuteAsync(ExecutionContext context, CancellationToken cancellationToken)
    {
        var configuration = context.Get<ConfigurationStepOutput>();
        var infrastructure = context.Get<InfrastructureStepOutput>();
        var fix = context.Get<FixStepOutput>();
        
        var container = $"incident-candidate-{configuration.Suffix}";
        
        _runtime = configuration.Runtime;
        _container = container;
        
        infrastructure.Environment["SERVICE_INSTANCE_ID"] = $"candidate-{Guid.NewGuid():N}";
        
        await configuration.Runtime.RunIncidentCandidate(
            container,
            configuration.Project,
            fix.RuntimeCandidate,
            configuration.OutputDirectory,
            configuration.Topic,
            configuration.Group,
            configuration.RunId,
            infrastructure.Environment["SERVICE_INSTANCE_ID"],
            configuration.CommandLogPath,
            cancellationToken);
        
        Artifacts.Phase(configuration.RunDirectory, "replaced");
        
        context.Set(new RedeployStepOutput(container));
    }

    public ValueTask DisposeAsync() =>
        _runtime is not null && _container is not null
            ? new ValueTask(_runtime.RemoveContainer(_container))
            : ValueTask.CompletedTask;
}

public sealed record VerificationStepOutput(PublishedRecord Probe, Dictionary<string, bool> Checks);

public sealed class VerificationStep : IStep
{
    public string Name => "Verifying recovery and finalizing evidence";

    public async Task ExecuteAsync(ExecutionContext context, CancellationToken cancellationToken)
    {
        var options = context.ExperimentOptions;
        
        var configuration = context.Get<ConfigurationStepOutput>();
        var infrastructure = context.Get<InfrastructureStepOutput>();
        var incident = context.Get<ExperimentSetupOutput>();
        var redeploy = context.Get<RedeployStepOutput>();
        
        var (probe, checks) = await infrastructure.Kafka!.VerifyRecovery(
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
        
        context.Set(new VerificationStepOutput(probe, checks));
    }
}

public sealed record PostMortemStepOutput;

public sealed class PostMortemStep : IStep
{
    public string Name => "Generating the post-mortem from verified evidence";

    public async Task ExecuteAsync(ExecutionContext context, CancellationToken cancellationToken)
    {
        var options = context.ExperimentOptions;
        var configuration = context.Get<ConfigurationStepOutput>();
        var fix = context.Get<FixStepOutput>();
        
        await Agent.WritePostMortem(
            configuration.Root,
            fix.Candidate,
            configuration.RunDirectory,
            options.Model ?? "",
            options.CredentialEnvironment ?? "",
            cancellationToken);
        
        Artifacts.Phase(configuration.RunDirectory, "reported");
        
        context.Set(new PostMortemStepOutput());
    }
}
