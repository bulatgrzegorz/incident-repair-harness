using System.Diagnostics;
using System.Text;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Microsoft.Extensions.Hosting;
using ProductWorker;

namespace ProductWorker.Tests;

public partial class ProductProcessingTests
{
    private static readonly ActivitySource Activities = new("ProductWorker.Tests");

    [ClassDataSource<FunctionalInfrastructure>(Shared = SharedType.PerTestSession)]
    public required FunctionalInfrastructure Infrastructure { get; init; }

    [Test]
    public async Task ValidPhysicalProductIsPersistedAndCommitted()
    {
        var ledger = await RunScenario("""{"productId":"P-1","productType":"Physical","price":100}""");

        await Assert.That(ledger.Records).HasSingleItem();
        await Assert.That(ledger.Records[0].Disposition).IsEqualTo("processed");
        await Assert.That(ledger.Records[0].NormalizedType).IsEqualTo("physical");
    }

    private async Task<LedgerDocument> RunScenario(params string[] payloads)
    {
        var id = Guid.NewGuid().ToString("N");
        var topic = $"products-{id}";
        var group = $"product-worker-{id}";
        var output = Path.Combine(Path.GetTempPath(), $"product-worker-{id}");
        using var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = Infrastructure.KafkaEndpoint }).Build();
        await admin.CreateTopicsAsync(
            [new TopicSpecification { Name = topic, NumPartitions = 1, ReplicationFactor = 1 }],
            new CreateTopicsOptions { OperationTimeout = TimeSpan.FromSeconds(10), RequestTimeout = TimeSpan.FromSeconds(10) });
        using var host = WorkerHost.Create(new WorkerSettings(
            Infrastructure.KafkaEndpoint, topic, group, output, "functional-tests", id, Infrastructure.OtlpEndpoint));

        try
        {
            using var startupCancellation = new CancellationTokenSource(TimeSpan.FromSeconds(15));
            await host.StartAsync(startupCancellation.Token);
            using var cancellation = new CancellationTokenSource(TimeSpan.FromSeconds(15));
            var deadline = Stopwatch.StartNew();

            using var activity = Activities.StartActivity(TestContext.Current!.Metadata.TestName);
            using var producer = new ProducerBuilder<Null, byte[]>(new ProducerConfig { BootstrapServers = Infrastructure.KafkaEndpoint }).Build();
            foreach (var payload in payloads)
            {
                var message = new Message<Null, byte[]> { Value = Encoding.UTF8.GetBytes(payload), Headers = new Headers() };
                message.Headers.Add("traceparent", Encoding.ASCII.GetBytes(activity!.Id!));
                message.Headers.Add("tracestate", null!);
                await producer.ProduceAsync(topic, message, cancellation.Token);
            }

            LedgerDocument? observedLedger = null;
            var observedOffset = -1L;
            while (deadline.Elapsed < TimeSpan.FromSeconds(15))
            {
                var path = Path.Combine(output, "processed-products.json");
                if (File.Exists(path))
                {
                    observedLedger = System.Text.Json.JsonSerializer.Deserialize<LedgerDocument>(
                        await File.ReadAllTextAsync(path),
                        new System.Text.Json.JsonSerializerOptions { PropertyNamingPolicy = System.Text.Json.JsonNamingPolicy.SnakeCaseLower });
                    observedOffset = await CommittedOffset(admin, group, topic);
                    if (observedLedger?.Records.Count == payloads.Length && observedOffset == payloads.Length)
                    {
                        return observedLedger;
                    }
                }

                await Task.Delay(100);
            }

            throw new TimeoutException(
                $"Expected {payloads.Length} dispositions and committed-next offset {payloads.Length}; " +
                $"observed {observedLedger?.Records.Count ?? 0} dispositions and offset {observedOffset}.");
        }
        finally
        {
            using var stopCancellation = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            await host.StopAsync(stopCancellation.Token);
            if (Directory.Exists(output))
            {
                Directory.Delete(output, recursive: true);
            }
        }
    }

    private static async Task<long> CommittedOffset(IAdminClient admin, string group, string topic)
    {
        try
        {
            return (await admin.ListConsumerGroupOffsetsAsync(
                    [new ConsumerGroupTopicPartitions(group, [new TopicPartition(topic, 0)])],
                    new ListConsumerGroupOffsetsOptions { RequestTimeout = TimeSpan.FromSeconds(5) }))
                .Single().Partitions.Single().Offset.Value;
        }
        catch (KafkaException)
        {
            return -1;
        }
    }
}
