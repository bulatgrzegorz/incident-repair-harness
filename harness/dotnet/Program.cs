using System.ComponentModel;
using Spectre.Console;
using Spectre.Console.Cli;

namespace IncidentHarness;

public static class Program
{
    public static Task<int> Main(string[] args)
    {
        var app = new CommandApp();
        app.Configure(config =>
        {
            config.SetApplicationName("incident-harness");
            config.SetApplicationVersion("0.1.0");
            config.SetExceptionHandler((exception, _) =>
            {
                ConsoleUi.Error(exception.Message);
                return 1;
            });
            config.AddCommand<DoctorCommand>("doctor").WithDescription("Check local prerequisites");
            config.AddCommand<PrepareCommand>("prepare").WithDescription("Prepare local execution images");
            config.AddCommand<InspectCommand>("inspect").WithDescription("Print a finalized verdict");
            config.AddCommand<SmokeCommand>("smoke").WithDescription("Reproduce the blocked Kafka consumer");
            config.AddCommand<RunCommand>("run").WithDescription("Run a repair experiment");
        });
        return app.RunAsync(args);
    }
}

public class AgentSettings : CommandSettings
{
    [CommandOption("--agent <AGENT>")]
    [DefaultValue("fixture")]
    public string Agent { get; init; } = "fixture";

    public override ValidationResult Validate() =>
        Agent is "fixture" or "opencode"
            ? ValidationResult.Success()
            : ValidationResult.Error("--agent must be fixture or opencode");
}

public sealed class PrepareSettings : CommandSettings
{
    [CommandOption("--agent <AGENT>", true)]
    public string Agent { get; init; } = "";

    public override ValidationResult Validate() =>
        Agent is "fixture" or "opencode"
            ? ValidationResult.Success()
            : ValidationResult.Error("--agent must be fixture or opencode");
}

public sealed class InspectSettings : CommandSettings
{
    [CommandOption("--run <RUN-ID>", true)]
    public string RunId { get; init; } = "";
}

public sealed class KeepSettings : CommandSettings
{
    [CommandOption("--keep")]
    [Description("Keep the broker and its volume")]
    public bool Keep { get; init; }
}

public sealed class RunSettings : AgentSettings
{
    [CommandOption("--model <MODEL>")]
    [Description("OpenCode provider and model")]
    public string? Model { get; init; }

    [CommandOption("--credential-env <NAME>")]
    [Description("Environment variable containing the provider credential")]
    public string? CredentialEnvironment { get; init; }

    [CommandOption("--keep")]
    [Description("Keep the broker and its volume")]
    public bool Keep { get; init; }
}

public sealed class DoctorCommand : AsyncCommand<AgentSettings>
{
    protected override Task<int> ExecuteAsync(CommandContext context, AgentSettings settings, CancellationToken cancellationToken) =>
        Doctor.Run(Repository.FindRoot(), settings.Agent, cancellationToken);
}

public sealed class PrepareCommand : AsyncCommand<PrepareSettings>
{
    protected override async Task<int> ExecuteAsync(CommandContext context, PrepareSettings settings, CancellationToken cancellationToken)
    {
        await Agent.Prepare(Repository.FindRoot(), settings.Agent, cancellationToken);
        return 0;
    }
}

public sealed class InspectCommand : Command<InspectSettings>
{
    protected override int Execute(CommandContext context, InspectSettings settings, CancellationToken cancellationToken) =>
        Artifacts.Inspect(Repository.FindRoot(), settings.RunId);
}

public sealed class SmokeCommand : AsyncCommand<KeepSettings>
{
    protected override Task<int> ExecuteAsync(CommandContext context, KeepSettings settings, CancellationToken cancellationToken) =>
        Experiment.Run(new ExperimentOptions(Repository.FindRoot(), settings.Keep, null, null, null), cancellationToken);
}

public sealed class RunCommand : AsyncCommand<RunSettings>
{
    protected override ValidationResult Validate(CommandContext context, RunSettings settings)
    {
        var agentValidation = settings.Validate();
        if (!agentValidation.Successful)
        {
            return agentValidation;
        }
        return settings.Agent != "opencode" || (settings.Model is not null && settings.CredentialEnvironment is not null)
            ? ValidationResult.Success()
            : ValidationResult.Error("--model and --credential-env are required for --agent opencode");
    }

    protected override Task<int> ExecuteAsync(CommandContext context, RunSettings settings, CancellationToken cancellationToken) =>
        Experiment.Run(
            new ExperimentOptions(
                Repository.FindRoot(),
                settings.Keep,
                settings.Agent,
                settings.Model,
                settings.CredentialEnvironment),
            cancellationToken);
}

public static class Repository
{
    public static string FindRoot()
    {
        foreach (var start in new[] { Environment.CurrentDirectory, AppContext.BaseDirectory })
        {
            for (var directory = new DirectoryInfo(start); directory is not null; directory = directory.Parent)
            {
                if (File.Exists(Path.Combine(directory.FullName, "global.json")) &&
                    Directory.Exists(Path.Combine(directory.FullName, "infrastructure")))
                {
                    return directory.FullName;
                }
            }
        }

        throw new DirectoryNotFoundException("Could not locate the incident-repair-harness repository root");
    }
}
