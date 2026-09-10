namespace ProductWorker;

public sealed record WorkerSettings(
    string BootstrapServers,
    string Topic,
    string GroupId,
    string OutputDirectory,
    string ServiceNamespace,
    string InstanceId,
    Uri? OtlpEndpoint = null);
