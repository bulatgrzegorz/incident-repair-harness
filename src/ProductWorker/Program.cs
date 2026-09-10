using System.Text.Json;
using Microsoft.Extensions.Hosting;
using ProductWorker;

if (args.Length == 1)
{
    try
    {
        var result = ProductProcessor.Process(System.Text.Encoding.UTF8.GetBytes(args[0]));
        Console.WriteLine(JsonSerializer.Serialize(result, new JsonSerializerOptions(JsonSerializerDefaults.Web)));
        return 0;
    }
    catch (Exception exception)
    {
        Console.Error.WriteLine($"{exception.GetType().Name}: {exception.Message}");
        return 1;
    }
}

var requiredSettings = new[]
{
    "KAFKA_BOOTSTRAP_SERVERS",
    "KAFKA_TOPIC",
    "KAFKA_GROUP_ID",
    "OUTPUT_DIRECTORY",
    "SERVICE_NAMESPACE",
    "SERVICE_INSTANCE_ID",
};

var missingSettings = requiredSettings.Where(name => string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable(name))).ToArray();
if (missingSettings.Length > 0)
{
    Console.Error.WriteLine($"Missing environment settings: {string.Join(", ", missingSettings)}");
    return 2;
}

using var cancellation = new CancellationTokenSource();
Console.CancelKeyPress += (_, eventArgs) =>
{
    eventArgs.Cancel = true;
    cancellation.Cancel();
};

var settings = new WorkerSettings(
    Environment.GetEnvironmentVariable("KAFKA_BOOTSTRAP_SERVERS")!,
    Environment.GetEnvironmentVariable("KAFKA_TOPIC")!,
    Environment.GetEnvironmentVariable("KAFKA_GROUP_ID")!,
    Environment.GetEnvironmentVariable("OUTPUT_DIRECTORY")!,
    Environment.GetEnvironmentVariable("SERVICE_NAMESPACE")!,
    Environment.GetEnvironmentVariable("SERVICE_INSTANCE_ID")!);

using var host = WorkerHost.Create(settings);
await host.RunAsync(cancellation.Token);
return 0;
