using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.RegularExpressions;
using System.Diagnostics;

namespace IncidentHarness;

public static partial class Agent
{
    public const string AgentImage = "localhost/incident-repair-agent:1.17.18";
    public const string ProxyImage = "docker.io/ubuntu/squid:6.13-25.04_beta@sha256:3de2e64f0ca6efdac3e98557607dc0f23050037f3885016d5d5bfcf9950501b8";

    public static async Task Prepare(string root, string agent, CancellationToken cancellationToken = default)
    {
        var timer = Stopwatch.StartNew();
        var runtime = await IncidentHarness.ContainerRuntime.Detect(cancellationToken);
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
        var provider = model.Split('/', 2)[0];
        if (provider is not ("openai" or "anthropic"))
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

        var runtime = await IncidentHarness.ContainerRuntime.Detect(cancellationToken);
        var suffix = Guid.NewGuid().ToString("N")[..8];
        var network = $"incident-agent-{suffix}";
        var proxy = $"incident-proxy-{suffix}";
        var coding = $"incident-coding-{suffix}";
        var config = JsonSerializer.Serialize(new
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
        var prompt = File.ReadAllText(Path.Combine(root, "agent/repair-prompt.md"));
        Directory.CreateDirectory(submission);
        try
        {
            await runtime.Run(["network", "create", "--internal", network], TimeSpan.FromSeconds(30), cancellationToken: cancellationToken);
            await RunProxyContainer(runtime, root, proxy, cancellationToken);
            await runtime.Run(["network", "connect", network, proxy], TimeSpan.FromSeconds(30), cancellationToken: cancellationToken);
            await Task.Delay(TimeSpan.FromSeconds(1), cancellationToken);

            var exitCode = await RunCodingContainer(
                runtime,
                coding,
                network,
                proxy,
                candidate,
                evidence,
                submission,
                artifacts,
                credentialEnvironment,
                config,
                model,
                prompt,
                cancellationToken);
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
                await File.WriteAllTextAsync(Path.Combine(artifacts, "proxy.log"), proxyLog.StandardOutput + proxyLog.StandardError);
            });
            await BestEffort(() => runtime.Run(["rm", "--force", coding], TimeSpan.FromSeconds(30), check: false));
            await BestEffort(() => runtime.Run(["rm", "--force", proxy], TimeSpan.FromSeconds(30), check: false));
            await BestEffort(() => runtime.Run(["network", "rm", network], TimeSpan.FromSeconds(30), check: false));
        }

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

    public static void ValidateSubmission(string submission)
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
        string credentialEnvironment,
        string config,
        string model,
        string prompt,
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
            "--volume", $"{candidate}:/workspace:rw",
            "--volume", $"{evidence}:/workspace/evidence:ro",
            "--volume", $"{submission}:/submission:rw",
            "--env", $"HTTP_PROXY=http://{proxy}:3128",
            "--env", $"HTTPS_PROXY=http://{proxy}:3128",
            "--env", "NO_PROXY=",
            "--env", credentialEnvironment,
            "--env", $"OPENCODE_CONFIG_CONTENT={config}",
            AgentImage, "opencode", "run", "--pure", "--format", "json", "--model", model, "--dir", "/workspace", prompt,
        ]);
        return runtime.RunToFiles(
            command,
            TimeSpan.FromMinutes(10),
            Path.Combine(artifacts, "repair-stdout.ndjson"),
            Path.Combine(artifacts, "repair-stderr.txt"),
            cancellationToken);
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
        }
    }

    [GeneratedRegex(@"^[A-Z][A-Z0-9_]*$")]
    private static partial Regex CredentialName();
}
