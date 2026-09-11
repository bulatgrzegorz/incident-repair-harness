namespace IncidentHarness;

public static class Doctor
{
    public static async Task<int> Run(string root, string agent, CancellationToken cancellationToken = default)
    {
        var checks = new List<(string Name, bool Available, string Detail)>();
        await AddVersion(checks, ".NET 10", "dotnet", ["--version"], cancellationToken, detail => detail.StartsWith("10.", StringComparison.Ordinal));
        await AddVersion(checks, "Git", "git", ["--version"], cancellationToken);
        await AddVersion(checks, "patch", "patch", ["--version"], cancellationToken);
        checks.Add(("worker project", File.Exists(Path.Combine(root, "src/ProductWorker/ProductWorker.csproj")), "present"));
        checks.Add(("functional test project", File.Exists(Path.Combine(root, "tests/ProductWorker.Tests/ProductWorker.Tests.csproj")), "present"));

        try
        {
            var runtime = await ContainerRuntime.Detect(cancellationToken);
            await AddVersion(checks, "container runtime", runtime.Executable, ["version", "--format", "{{.Server.Version}}"], cancellationToken);
            await AddVersion(checks, "Compose", runtime.Executable, ["compose", "version"], cancellationToken);
            await AddVersion(checks, "agent image", runtime.Executable, ["image", "inspect", "--format", "{{.Id}}", Agent.AgentImage], cancellationToken);
            if (agent == "opencode")
            {
                await AddVersion(checks, "proxy image", runtime.Executable, ["image", "inspect", "--format", "{{.Id}}", Agent.ProxyImage], cancellationToken);
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception exception)
        {
            checks.Add(("container runtime", false, exception.Message));
        }

        ConsoleUi.Checks(checks);
        return checks.Any(check => !check.Available) ? 1 : 0;
    }

    private static async Task AddVersion(
        List<(string Name, bool Available, string Detail)> checks,
        string name,
        string executable,
        string[] arguments,
        CancellationToken cancellationToken,
        Func<string, bool>? validate = null)
    {
        try
        {
            var result = await ProcessRunner.Run(executable, arguments, TimeSpan.FromSeconds(10), check: false, cancellationToken: cancellationToken);
            var detail = (string.IsNullOrWhiteSpace(result.StandardOutput) ? result.StandardError : result.StandardOutput).Trim();
            checks.Add((name, result.ExitCode == 0 && (validate?.Invoke(detail) ?? true), detail.Length == 0 ? $"exit {result.ExitCode}" : detail));
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception exception)
        {
            checks.Add((name, false, exception.Message));
        }
    }
}
