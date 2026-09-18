namespace IncidentHarness;

public sealed record ExperimentOptions(
    string Root,
    bool Keep,
    string? Agent,
    string? Model,
    string? CredentialEnvironment);

public static class Experiment
{
    public static Task<int> Run(ExperimentOptions options, CancellationToken cancellationToken = default) =>
        new StepExecutionService().Execute(options, cancellationToken);
}
