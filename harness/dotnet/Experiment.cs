using System.Diagnostics;

namespace IncidentHarness;

public sealed record ExperimentOptions(
    string Root,
    bool Keep,
    string? Agent,
    string? Model,
    string? CredentialEnvironment);

public sealed class ExperimentContext(ExperimentOptions experimentOptions)
{
    public ExperimentOptions ExperimentOptions { get; } = experimentOptions;

    // Steps populate these in the order defined by CreateSteps.
    public ConfigurationStepOutput Configuration { get; set; } = null!;
    public InfrastructureStepOutput Infrastructure { get; set; } = null!;
    public ExperimentSetupOutput Incident { get; set; } = null!;
    public FixStepOutput Fix { get; set; } = null!;
    public RedeployStepOutput Redeployment { get; set; } = null!;
}

public interface IStep : IAsyncDisposable
{
    string Name { get; }
    Task ExecuteAsync(ExperimentContext context, CancellationToken cancellationToken);
    ValueTask IAsyncDisposable.DisposeAsync() => ValueTask.CompletedTask;
}

public static class Experiment
{
    public static async Task<int> Run(ExperimentOptions experimentOptions, CancellationToken cancellationToken = default)
    {
        var context = new ExperimentContext(experimentOptions);
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

            var configuration = context.Configuration;
            if (experimentOptions.Agent is null)
            {
                var incident = context.Incident;
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

    private static string ArtifactLocation(ExperimentContext context) =>
        context.Configuration is { } configuration
            ? $"  Artifacts: {configuration.RunDirectory}"
            : "";

    internal static async Task BestEffort(Func<Task> action)
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
