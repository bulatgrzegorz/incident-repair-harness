using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.RegularExpressions;
using System.Diagnostics;

namespace IncidentHarness;

public static partial class Agent
{
    public const string AgentImage = "localhost/incident-repair-agent:1.17.18";
    public const string ProxyImage = "docker.io/ubuntu/squid:6.13-25.04_beta";

    public static async Task Prepare(string root, string agent, CancellationToken cancellationToken = default)
    {
        var timer = Stopwatch.StartNew();
        var runtime = await ContainerRuntime.Detect(cancellationToken);
        ConsoleUi.PreparationHeader(agent, runtime.Executable);
        await ConsoleUi.Status(
            $"Building {AgentImage}",
            () => runtime.Run([
                "build", "--file", Path.Combine(root, "infrastructure/Agent.Dockerfile"), "--tag", AgentImage, root,
            ], TimeSpan.FromMinutes(10), cancellationToken: cancellationToken));

        var images = new List<(string Image, string Purpose)>
        {
            (AgentImage, "candidate build, test, and runtime"),
        };
        if (agent == "opencode")
        {
            await ConsoleUi.Status(
                $"Pulling {ProxyImage}",
                () => runtime.Run(["pull", ProxyImage], TimeSpan.FromMinutes(5), cancellationToken: cancellationToken));
            images.Add((ProxyImage, "provider-only network proxy"));
        }
        ConsoleUi.Prepared(images, timer.Elapsed);
    }

    public static async Task RunOpenCode(
        string root,
        string candidate,
        string evidence,
        string submission,
        string artifacts,
        string model,
        string credentialEnvironment,
        CancellationToken cancellationToken = default)
    {
        ValidateProviderAccess(model, credentialEnvironment);
        var prompt = await File.ReadAllTextAsync(Path.Combine(root, "agent/repair-prompt.md"), cancellationToken);
        var state = Path.Combine(artifacts, "opencode-state");
        Directory.CreateDirectory(submission);
        Directory.CreateDirectory(state);
        await RunRestrictedOpenCode(
            root, candidate, evidence, submission, artifacts, state, model, credentialEnvironment,
            prompt, "repair", null, cancellationToken);

        var sessions = File.ReadLines(Path.Combine(artifacts, "repair-stdout.ndjson"))
            .Select(ParseSession)
            .Where(session => session is not null)
            .ToHashSet(StringComparer.Ordinal);
        if (sessions.Count != 1)
        {
            throw new InvalidOperationException($"Expected one OpenCode session, received {sessions.Count}");
        }
        Artifacts.WriteJson(Path.Combine(artifacts, "session.json"), new { session_id = sessions.Single() });

        ValidateSubmission(submission);
    }

    public static async Task WritePostMortem(
        string root,
        string candidate,
        string runDirectory,
        string model,
        string credentialEnvironment,
        CancellationToken cancellationToken = default)
    {
        ValidateProviderAccess(model, credentialEnvironment);
        var artifacts = Path.Combine(runDirectory, "agent");
        var submission = Path.Combine(runDirectory, "submission");
        var evidence = Path.Combine(runDirectory, "agent-evidence");
        foreach (var name in new[] { "incident.json", "source.diff", "verification.json", "test-results.json" })
        {
            File.Copy(Path.Combine(runDirectory, name), Path.Combine(evidence, name), overwrite: true);
        }

        var session = Artifacts.ReadJson(Path.Combine(artifacts, "session.json"))["session_id"]?.GetValue<string>()
            ?? throw new JsonException("Missing OpenCode session ID");
        var state = Path.Combine(artifacts, "opencode-state");
        var prompt = await File.ReadAllTextAsync(Path.Combine(root, "agent/post-mortem-prompt.md"), cancellationToken);
        await RunRestrictedOpenCode(
            root, candidate, evidence, submission, artifacts, state, model, credentialEnvironment,
            prompt, "post-mortem", session, cancellationToken);

        var report = Path.Combine(submission, "post-mortem.md");
        ValidatePostMortem(report);
        File.Move(report, Path.Combine(runDirectory, "post-mortem.md"), overwrite: true);
        Directory.Delete(state, recursive: true);
    }

    internal static void ValidateSubmission(string submission)
    {
        var summaryPath = Path.Combine(submission, "repair-summary.json");
        if (!File.Exists(summaryPath))
        {
            return;
        }
        var summary = Artifacts.ReadJson(summaryPath);
        if (!IsString(summary["diagnosis"]) || !IsString(summary["regression"]) ||
            !IsString(summary["test_result"]) || summary["changed_files"] is not JsonArray changed ||
            changed.Any(path => !IsString(path)))
        {
            throw new JsonException("Invalid repair-summary.json");
        }
    }

    internal static void ValidatePostMortem(string path)
    {
        if (!File.Exists(path))
        {
            throw new FileNotFoundException("Agent did not submit post-mortem.md");
        }
        var report = File.ReadAllText(path);
        foreach (var heading in new[] { "Summary", "Impact", "Timeline", "Root Cause", "Resolution", "Validation", "Follow-up Actions" })
        {
            if (!report.Contains($"## {heading}", StringComparison.Ordinal))
            {
                throw new InvalidDataException($"post-mortem.md is missing the '{heading}' section");
            }
        }
    }

    private static async Task RunRestrictedOpenCode(
        string root,
        string candidate,
        string evidence,
        string submission,
        string artifacts,
        string state,
        string model,
        string credentialEnvironment,
        string prompt,
        string outputPrefix,
        string? session,
        CancellationToken cancellationToken)
    {
        var runtime = await ContainerRuntime.Detect(cancellationToken);
        var suffix = Guid.NewGuid().ToString("N")[..8];
        var network = $"incident-agent-{suffix}";
        var proxy = $"incident-proxy-{suffix}";
        var coding = $"incident-coding-{suffix}";
        try
        {
            await runtime.Run(["network", "create", "--internal", network], TimeSpan.FromSeconds(30), cancellationToken: cancellationToken);
            await RunProxyContainer(runtime, root, proxy, cancellationToken);
            await runtime.Run(["network", "connect", network, proxy], TimeSpan.FromSeconds(30), cancellationToken: cancellationToken);
            await Task.Delay(TimeSpan.FromSeconds(1), cancellationToken);
            var exitCode = await RunCodingContainer(
                runtime, coding, network, proxy, candidate, evidence, submission, artifacts, state,
                credentialEnvironment, CreateConfig(model), model, prompt, outputPrefix, session, cancellationToken);
            if (exitCode != 0)
            {
                throw new InvalidOperationException($"OpenCode exited with {exitCode}");
            }
        }
        finally
        {
            await BestEffort(async () =>
            {
                var proxyLog = await runtime.Run(["logs", proxy], TimeSpan.FromSeconds(30), check: false);
                var name = outputPrefix == "repair" ? "proxy.log" : $"{outputPrefix}-proxy.log";
                await File.WriteAllTextAsync(Path.Combine(artifacts, name), proxyLog.StandardOutput + proxyLog.StandardError);
            });
            await BestEffort(() => runtime.Run(["rm", "--force", coding], TimeSpan.FromSeconds(30), check: false));
            await BestEffort(() => runtime.Run(["rm", "--force", proxy], TimeSpan.FromSeconds(30), check: false));
            await BestEffort(() => runtime.Run(["network", "rm", network], TimeSpan.FromSeconds(30), check: false));
        }
    }

    private static Task<CommandResult> RunProxyContainer(
        ContainerRuntime runtime,
        string root,
        string proxy,
        CancellationToken cancellationToken) =>
        runtime.Run([
            "run", "--detach", "--name", proxy, "--network", runtime.IsPodman ? "podman" : "bridge",
            "--volume", $"{Path.Combine(root, "infrastructure/squid.conf")}:/etc/squid/squid.conf:ro", ProxyImage,
        ], TimeSpan.FromSeconds(30), cancellationToken: cancellationToken);

    private static Task<int> RunCodingContainer(
        ContainerRuntime runtime,
        string container,
        string network,
        string proxy,
        string candidate,
        string evidence,
        string submission,
        string artifacts,
        string state,
        string credentialEnvironment,
        string config,
        string model,
        string prompt,
        string outputPrefix,
        string? session,
        CancellationToken cancellationToken)
    {
        var command = new List<string>
        {
            "run", "--name", container, "--network", network, "--read-only",
        };
        command.AddRange(runtime.UserArguments);
        command.AddRange([
            "--cap-drop=all", "--security-opt=no-new-privileges", "--pids-limit=256", "--memory=3g",
            "--tmpfs", "/tmp:rw,size=512m", "--tmpfs", "/home/agent:rw,mode=1777,size=512m",
            "--volume", $"{candidate}:/workspace:{(session is null ? "rw" : "ro")}",
            "--volume", $"{evidence}:/workspace/evidence:ro",
            "--volume", $"{submission}:/submission:rw",
            "--volume", $"{state}:/opencode-state:rw",
            "--env", $"HTTP_PROXY=http://{proxy}:3128",
            "--env", $"HTTPS_PROXY=http://{proxy}:3128",
            "--env", "NO_PROXY=",
            "--env", "XDG_DATA_HOME=/opencode-state",
            "--env", credentialEnvironment,
            "--env", $"OPENCODE_CONFIG_CONTENT={config}",
            AgentImage, "opencode", "run", "--pure", "--format", "json", "--model", model, "--dir", "/workspace",
        ]);
        if (session is not null)
        {
            command.AddRange(["--session", session]);
        }
        command.Add(prompt);
        return runtime.RunToFiles(
            command,
            TimeSpan.FromMinutes(10),
            Path.Combine(artifacts, $"{outputPrefix}-stdout.ndjson"),
            Path.Combine(artifacts, $"{outputPrefix}-stderr.txt"),
            cancellationToken);
    }

    private static string CreateConfig(string model)
    {
        var provider = model.Split('/', 2)[0];
        return JsonSerializer.Serialize(new
        {
            autoupdate = false,
            share = "disabled",
            snapshot = false,
            plugin = Array.Empty<string>(),
            mcp = new { },
            enabled_providers = new[] { provider },
            model,
            small_model = model,
            permission = new Dictionary<string, object>
            {
                ["webfetch"] = "deny",
                ["websearch"] = "deny",
                ["codesearch"] = "deny",
                ["task"] = "deny",
                ["external_directory"] = new Dictionary<string, string> { ["/submission/**"] = "allow" },
            },
        });
    }

    private static void ValidateProviderAccess(string model, string credentialEnvironment)
    {
        if (model.Split('/', 2)[0] is not ("openai" or "anthropic"))
        {
            throw new ArgumentException("Only openai/* and anthropic/* are allowlisted");
        }
        if (!CredentialName().IsMatch(credentialEnvironment))
        {
            throw new ArgumentException("Invalid credential environment variable name");
        }
        if (string.IsNullOrEmpty(Environment.GetEnvironmentVariable(credentialEnvironment)))
        {
            throw new ArgumentException($"Missing credential environment variable: {credentialEnvironment}");
        }
    }

    private static string? ParseSession(string line)
    {
        try
        {
            return JsonNode.Parse(line)?["sessionID"]?.GetValue<string>();
        }
        catch (JsonException)
        {
            return null;
        }
    }

    private static bool IsString(JsonNode? node) => node is JsonValue value && value.TryGetValue<string>(out _);

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

    [GeneratedRegex(@"^[A-Z][A-Z0-9_]*$")]
    private static partial Regex CredentialName();
}
