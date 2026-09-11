namespace IncidentHarness;

public sealed class ContainerRuntime
{
    private ContainerRuntime(string executable) => Executable = executable;

    public string Executable { get; }

    public bool IsPodman => Executable == "podman";

    public string[] UserArguments =>
        IsPodman
            ? ["--userns=keep-id", "--user", UnixHost.User]
            : ["--user", UnixHost.User];

    public static async Task<ContainerRuntime> Detect(CancellationToken cancellationToken = default)
    {
        foreach (var executable in new[] { "docker", "podman" })
        {
            if (await ProcessRunner.Works(executable, cancellationToken, "version"))
            {
                return new ContainerRuntime(executable);
            }
        }
        throw new InvalidOperationException("A working Docker or Podman runtime is required");
    }

    public Task<CommandResult> Run(
        IEnumerable<string> arguments,
        TimeSpan timeout,
        bool check = true,
        string? logPath = null,
        CancellationToken cancellationToken = default) =>
        ProcessRunner.Run(Executable, arguments, timeout, check: check, logPath: logPath, cancellationToken: cancellationToken);

    public Task<int> RunToFiles(
        IEnumerable<string> arguments,
        TimeSpan timeout,
        string stdoutPath,
        string stderrPath,
        CancellationToken cancellationToken = default) =>
        ProcessRunner.RunToFiles(Executable, arguments, timeout, stdoutPath, stderrPath, cancellationToken: cancellationToken);

    public Task<CommandResult> StartInfrastructure(
        string root,
        string project,
        string commandLog,
        CancellationToken cancellationToken) =>
        Run(
            ["compose", "-p", project, "-f", Path.Combine(root, "infrastructure/compose.yaml"), "up", "-d", "--wait"],
            TimeSpan.FromSeconds(120),
            logPath: commandLog,
            cancellationToken: cancellationToken);

    public Task<CommandResult> StopInfrastructure(string root, string project, string commandLog) =>
        Run(
            ["compose", "-p", project, "-f", Path.Combine(root, "infrastructure/compose.yaml"), "down", "--volumes"],
            TimeSpan.FromSeconds(60),
            check: false,
            logPath: commandLog);

    public async Task RunIncidentCandidate(
        string container,
        string project,
        string candidate,
        string outputDirectory,
        string topic,
        string group,
        string runId,
        string instanceId,
        string commandLog,
        CancellationToken cancellationToken)
    {
        var command = new List<string>
        {
            "run", "--detach", "--name", container, "--network", $"{project}_application", "--read-only",
        };
        command.AddRange(UserArguments);
        command.AddRange([
            "--cap-drop=all", "--security-opt=no-new-privileges", "--pids-limit=256", "--memory=1g",
            "--tmpfs", "/tmp:rw,size=128m", "--tmpfs", "/home/agent:rw,mode=1777,size=128m",
            "--volume", $"{candidate}:/workspace:ro", "--volume", $"{outputDirectory}:/output:rw",
            "--env", "KAFKA_BOOTSTRAP_SERVERS=broker:19092", "--env", $"KAFKA_TOPIC={topic}",
            "--env", $"KAFKA_GROUP_ID={group}", "--env", "OUTPUT_DIRECTORY=/output",
            "--env", $"SERVICE_NAMESPACE={runId}", "--env", $"SERVICE_INSTANCE_ID={instanceId}",
            "--env", "OTEL_EXPORTER_OTLP_ENDPOINT=http://lgtm:4318", "--env", "OTEL_EXPORTER_OTLP_PROTOCOL=http/protobuf",
            Agent.AgentImage, "dotnet", "run", "--project", "src/ProductWorker/ProductWorker.csproj", "--configuration", "Release", "--no-build",
        ]);
        await Run(command, TimeSpan.FromSeconds(30), logPath: commandLog, cancellationToken: cancellationToken);
    }

    public async Task CaptureLogs(string container, string output, CancellationToken cancellationToken)
    {
        var logs = await Run(["logs", container], TimeSpan.FromSeconds(30), check: false, cancellationToken: cancellationToken);
        await File.WriteAllTextAsync(output, logs.StandardOutput + logs.StandardError, cancellationToken);
    }

    public async Task RemoveContainer(string container) =>
        _ = await Run(["rm", "--force", container], TimeSpan.FromSeconds(30), check: false);
}
