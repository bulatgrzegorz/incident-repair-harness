using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using OpenTelemetry;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;
using Testcontainers.Kafka;
using TUnit.Core.Interfaces;

namespace ProductWorker.Tests;

public sealed class FunctionalInfrastructure : IAsyncInitializer, IAsyncDisposable
{
    private const string KafkaImage = "docker.io/apache/kafka:4.0.0";
    private const string AspireImage = "mcr.microsoft.com/dotnet/aspire-dashboard:13.5.2";
    private KafkaContainer? _kafka;
    private IContainer? _aspire;
    private TracerProvider? _tracing;

    public string KafkaEndpoint { get; private set; } = null!;
    public Uri? OtlpEndpoint { get; private set; }

    public async Task InitializeAsync()
    {
        if (Environment.GetEnvironmentVariable("FUNCTIONAL_TESTS_MODE") == "External")
        {
            KafkaEndpoint = Required("FUNCTIONAL_TESTS_KAFKA_ENDPOINT");
            OtlpEndpoint = new Uri(Required("FUNCTIONAL_TESTS_OTLP_ENDPOINT"));
        }
        else
        {
            _kafka = new KafkaBuilder(KafkaImage).Build();
            _aspire = new ContainerBuilder(AspireImage)
                .WithEnvironment("ASPIRE_DASHBOARD_UNSECURED_ALLOW_ANONYMOUS", "true")
                .WithPortBinding(18888, true)
                .WithPortBinding(18889, true)
                .WithWaitStrategy(Wait.ForUnixContainer().UntilHttpRequestIsSucceeded(request => request.ForPort(18888)))
                .Build();

            await Task.WhenAll(_kafka.StartAsync(), _aspire.StartAsync());
            KafkaEndpoint = _kafka.GetBootstrapAddress();
            OtlpEndpoint = new Uri($"http://{_aspire.Hostname}:{_aspire.GetMappedPublicPort(18889)}");
        }

        _tracing = Sdk.CreateTracerProviderBuilder()
            .SetResourceBuilder(ResourceBuilder.CreateDefault().AddService("product-worker-tests"))
            .AddSource("ProductWorker.Tests")
            .AddOtlpExporter(options => options.Endpoint = OtlpEndpoint)
            .Build();
    }

    public async ValueTask DisposeAsync()
    {
        _tracing?.Dispose();

        if (_aspire is not null)
        {
            await _aspire.DisposeAsync();
        }

        if (_kafka is not null)
        {
            await _kafka.DisposeAsync();
        }
    }

    private static string Required(string name) =>
        Environment.GetEnvironmentVariable(name) is { Length: > 0 } value
            ? value
            : throw new InvalidOperationException($"{name} is required in External mode.");
}
