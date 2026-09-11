using System.Security.Cryptography;
using System.Text.Json;
using System.Text.Json.Nodes;

namespace IncidentHarness;

public sealed record CandidateFile(string Path, int Size, string Sha256);

public static class CandidateWorkspace
{
    private static readonly HashSet<string> AllowedChanges =
    [
        "src/ProductWorker/ProductProcessor.cs",
        "tests/ProductWorker.Smoke/Program.cs",
    ];

    public static async Task<string> Prepare(
        string root,
        string runDirectory,
        string agent,
        string? model,
        string? credentialEnvironment,
        string commandLog,
        CancellationToken cancellationToken = default)
    {
        var candidate = Path.Combine(runDirectory, "candidate");
        CopyWorkspace(root, root, candidate);
        var patch = Path.Combine(root, "fixtures/known-good.patch");
        if (agent == "fixture")
        {
            await ProcessRunner.Run("patch", ["-p1", "-i", patch], TimeSpan.FromSeconds(30), candidate, logPath: commandLog, cancellationToken: cancellationToken);
            File.Copy(patch, Path.Combine(runDirectory, "source.diff"));
        }
        else
        {
            var originalFiles = TextFiles(candidate);
            var evidence = Path.Combine(runDirectory, "agent-evidence");
            Directory.CreateDirectory(evidence);
            File.Copy(Path.Combine(runDirectory, "telemetry/alert.json"), Path.Combine(evidence, "alert.json"));
            var agentArtifacts = Path.Combine(runDirectory, "agent");
            Directory.CreateDirectory(agentArtifacts);
            await Agent.RunOpenCode(
                root,
                candidate,
                evidence,
                Path.Combine(runDirectory, "submission"),
                agentArtifacts,
                model ?? "",
                credentialEnvironment ?? "",
                cancellationToken);

            Manifest(candidate);
            CleanBuildOutputs(candidate);
            var currentFiles = TextFiles(candidate);
            var changed = originalFiles.Keys.Union(currentFiles.Keys)
                .Where(path => originalFiles.GetValueOrDefault(path) != currentFiles.GetValueOrDefault(path))
                .ToHashSet(StringComparer.Ordinal);
            if (!changed.SetEquals(AllowedChanges))
            {
                throw new InvalidOperationException($"Agent changed invalid paths or omitted its regression: {string.Join(", ", changed.Order())}");
            }

            var diff = new List<string>();
            foreach (var path in changed.Order(StringComparer.Ordinal))
            {
                diff.Add(await UnifiedDiff(path, originalFiles[path], Path.Combine(candidate, path), cancellationToken));
            }
            await File.WriteAllTextAsync(Path.Combine(runDirectory, "source.diff"), string.Concat(diff), cancellationToken);
        }

        Artifacts.WriteJson(Path.Combine(runDirectory, "candidate-source-manifest.json"), Manifest(candidate));
        return candidate;
    }

    public static async Task<string> Test(
        ContainerRuntime runtime,
        string root,
        string candidate,
        string runDirectory,
        CancellationToken cancellationToken = default)
    {
        var redControl = Path.Combine(runDirectory, "red-control");
        CopyWorkspace(root, candidate, redControl);
        var redOutput = Path.Combine(runDirectory, "red-control.log");
        var redResult = await RunSmokeTests(runtime, redControl, false, redOutput, cancellationToken);
        if (redResult.ExitCode == 0 || !File.ReadAllText(redOutput).Contains("NullReferenceException", StringComparison.Ordinal))
        {
            throw new InvalidOperationException("Regression check was not red against the original processor");
        }

        await RunSmokeTests(runtime, candidate, true, Path.Combine(runDirectory, "candidate-test.log"), cancellationToken);

        var policyResults = new JsonArray();
        foreach (var payload in new[]
        {
            "{\"productId\":\"P-policy\",\"price\":10}",
            "{\"productId\":\"P-policy\",\"productType\":null,\"price\":10}",
            "{\"productId\":\"P-policy\",\"productType\":\"\",\"price\":10}",
            "{\"productId\":\"P-policy\",\"productType\":\"  \",\"price\":10}",
        })
        {
            var result = await RunPolicyCheck(runtime, candidate, payload, cancellationToken);
            var line = result.StandardOutput.Split('\n', StringSplitOptions.RemoveEmptyEntries).Last();
            var policy = JsonNode.Parse(line) ?? throw new InvalidOperationException("Candidate returned empty policy JSON");
            if (policy["disposition"]?.GetValue<string>() != "rejected" || policy["reasonCode"]?.GetValue<string>() != "missing_product_type")
            {
                throw new InvalidOperationException($"Malformed-type policy failed: {policy}");
            }
            policyResults.Add(policy);
        }
        Artifacts.WriteJson(Path.Combine(runDirectory, "policy-results.json"), policyResults);

        CleanBuildOutputs(candidate);
        var frozen = JsonSerializer.Deserialize<CandidateFile[]>(
            File.ReadAllText(Path.Combine(runDirectory, "candidate-source-manifest.json")),
            Artifacts.JsonOptions) ?? throw new JsonException("Candidate source manifest is empty");
        if (!frozen.SequenceEqual(Manifest(candidate)))
        {
            throw new InvalidOperationException("Candidate source changed after it was frozen");
        }
        var runtimeCandidate = Path.Combine(runDirectory, "candidate-runtime");
        CopyWorkspace(candidate, candidate, runtimeCandidate);
        await RestoreWorker(runtime, runtimeCandidate, Path.Combine(runDirectory, "candidate-restore.log"), cancellationToken);
        await BuildWorker(runtime, runtimeCandidate, Path.Combine(runDirectory, "candidate-build.log"), cancellationToken);
        return runtimeCandidate;
    }

    public static CandidateFile[] Manifest(string directory)
    {
        var files = new List<CandidateFile>();
        var pending = new Stack<string>();
        pending.Push(directory);
        while (pending.TryPop(out var current))
        {
            foreach (var path in Directory.EnumerateFileSystemEntries(current).Order(StringComparer.Ordinal))
            {
                var relative = Relative(directory, path);
                var attributes = File.GetAttributes(path);
                if ((attributes & FileAttributes.ReparsePoint) != 0)
                {
                    throw new InvalidOperationException($"Candidate contains a symlink: {relative}");
                }
                if ((attributes & FileAttributes.Directory) != 0)
                {
                    if (Path.GetFileName(path) is not ("bin" or "obj"))
                    {
                        pending.Push(path);
                    }
                    continue;
                }

                var info = new FileInfo(path);
                if (UnixHost.HardLinkCount(path) != 1 || info.Length > 1_000_000)
                {
                    throw new InvalidOperationException($"Candidate contains an unusual file: {relative}");
                }
                var content = File.ReadAllBytes(path);
                files.Add(new CandidateFile(relative, content.Length, Convert.ToHexStringLower(SHA256.HashData(content))));
            }
        }
        return files.OrderBy(file => file.Path, StringComparer.Ordinal).ToArray();
    }

    public static void CleanBuildOutputs(string directory)
    {
        foreach (var path in Directory.EnumerateDirectories(directory, "*", SearchOption.AllDirectories)
                     .Where(path => Path.GetFileName(path) is "bin" or "obj")
                     .OrderByDescending(path => path.Length)
                     .ToArray())
        {
            if (Directory.Exists(path))
            {
                Directory.Delete(path, recursive: true);
            }
        }
    }

    public static void CopyWorkspace(string sourceRoot, string testsRoot, string destination)
    {
        CopyDirectory(Path.Combine(sourceRoot, "src"), Path.Combine(destination, "src"));
        CopyDirectory(Path.Combine(testsRoot, "tests"), Path.Combine(destination, "tests"));
        foreach (var name in new[] { "Directory.Build.props", "Directory.Packages.props", "global.json" })
        {
            File.Copy(Path.Combine(sourceRoot, name), Path.Combine(destination, name));
        }
    }

    private static async Task<CommandResult> ContainerDotnet(
        ContainerRuntime runtime,
        string directory,
        IEnumerable<string> arguments,
        bool check,
        string? output = null,
        CancellationToken cancellationToken = default)
    {
        var command = new List<string> { "run", "--rm", "--network", "none", "--read-only" };
        command.AddRange(runtime.UserArguments);
        command.AddRange([
            "--cap-drop=all", "--security-opt=no-new-privileges", "--pids-limit=256", "--memory=3g",
            "--tmpfs", "/tmp:rw,size=512m", "--tmpfs", "/home/agent:rw,mode=1777,size=256m",
            "--volume", $"{directory}:/workspace:rw", Agent.AgentImage, "dotnet",
        ]);
        command.AddRange(arguments);
        if (output is null)
        {
            return await runtime.Run(command, TimeSpan.FromSeconds(120), check: check, cancellationToken: cancellationToken);
        }

        var stderr = output + ".stderr";
        var exitCode = await runtime.RunToFiles(command, TimeSpan.FromSeconds(120), output, stderr, cancellationToken);
        await using (var destination = new FileStream(output, FileMode.Append, FileAccess.Write, FileShare.Read))
        await using (var source = new FileStream(stderr, FileMode.Open, FileAccess.Read, FileShare.Read))
        {
            await source.CopyToAsync(destination);
        }
        File.Delete(stderr);
        if (check && exitCode != 0)
        {
            throw new InvalidOperationException($"Candidate dotnet command exited with {exitCode}; see {output}");
        }
        return new CommandResult(exitCode, "", "");
    }

    private static Task<CommandResult> RunSmokeTests(
        ContainerRuntime runtime,
        string directory,
        bool check,
        string output,
        CancellationToken cancellationToken) =>
        ContainerDotnet(
            runtime,
            directory,
            ["run", "--project", "tests/ProductWorker.Smoke/ProductWorker.Smoke.csproj", "--configuration", "Release", "--property:RestoreLockedMode=true"],
            check,
            output,
            cancellationToken);

    private static Task<CommandResult> RunPolicyCheck(
        ContainerRuntime runtime,
        string directory,
        string payload,
        CancellationToken cancellationToken) =>
        ContainerDotnet(
            runtime,
            directory,
            ["run", "--project", "src/ProductWorker/ProductWorker.csproj", "--configuration", "Release", "--no-build", "--", payload],
            true,
            cancellationToken: cancellationToken);

    private static Task<CommandResult> RestoreWorker(
        ContainerRuntime runtime,
        string directory,
        string output,
        CancellationToken cancellationToken) =>
        ContainerDotnet(runtime, directory, ["restore", "src/ProductWorker/ProductWorker.csproj", "--locked-mode"], true, output, cancellationToken);

    private static Task<CommandResult> BuildWorker(
        ContainerRuntime runtime,
        string directory,
        string output,
        CancellationToken cancellationToken) =>
        ContainerDotnet(
            runtime,
            directory,
            ["build", "src/ProductWorker/ProductWorker.csproj", "--configuration", "Release", "--no-restore"],
            true,
            output,
            cancellationToken);

    private static void CopyDirectory(string source, string destination)
    {
        Directory.CreateDirectory(destination);
        foreach (var file in Directory.EnumerateFiles(source))
        {
            if (Path.GetFileName(file) != ".DS_Store")
            {
                File.Copy(file, Path.Combine(destination, Path.GetFileName(file)));
            }
        }
        foreach (var directory in Directory.EnumerateDirectories(source))
        {
            if (Path.GetFileName(directory) is not ("bin" or "obj"))
            {
                CopyDirectory(directory, Path.Combine(destination, Path.GetFileName(directory)));
            }
        }
    }

    private static Dictionary<string, string> TextFiles(string directory) =>
        Directory.EnumerateFiles(directory, "*", SearchOption.AllDirectories)
            .Where(path => !HasDirectory(path, "bin") && !HasDirectory(path, "obj"))
            .ToDictionary(path => Relative(directory, path), File.ReadAllText, StringComparer.Ordinal);

    private static bool HasDirectory(string path, string name) =>
        path.Split(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar).Contains(name, StringComparer.Ordinal);

    private static string Relative(string root, string path) =>
        Path.GetRelativePath(root, path).Replace(Path.DirectorySeparatorChar, '/');

    private static async Task<string> UnifiedDiff(string path, string original, string currentPath, CancellationToken cancellationToken)
    {
        var temporary = Directory.CreateTempSubdirectory("incident-harness-diff-");
        try
        {
            var originalPath = Path.Combine(temporary.FullName, Path.GetFileName(path));
            await File.WriteAllTextAsync(originalPath, original, cancellationToken);
            var result = await ProcessRunner.Run(
                "git",
                ["diff", "--no-index", "--no-ext-diff", "--no-color", "--", originalPath, currentPath],
                TimeSpan.FromSeconds(30),
                check: false,
                cancellationToken: cancellationToken);
            if (result.ExitCode != 1)
            {
                throw new InvalidOperationException($"Could not generate source diff for {path}: {result.StandardError.Trim()}");
            }

            var lines = result.StandardOutput.Split('\n');
            if (lines.Length < 4)
            {
                throw new InvalidOperationException($"Git returned an invalid source diff for {path}");
            }
            lines[0] = $"diff --git a/{path} b/{path}";
            lines[2] = $"--- a/{path}";
            lines[3] = $"+++ b/{path}";
            return string.Join('\n', lines);
        }
        finally
        {
            temporary.Delete(recursive: true);
        }
    }
}
