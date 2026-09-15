using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Confluent.Kafka;
using Confluent.Kafka.Admin;

namespace IncidentHarness;

public sealed record PublishedRecord(string Topic, int Partition, long Offset, string PayloadSha256, string Payload);

public sealed record LedgerRecord(
    string Topic,
    int Partition,
    long Offset,
    string PayloadSha256,
    string Disposition,
    string ProductId,
    string? NormalizedType,
    decimal? Price,
    string? ReasonCode);

public sealed record LedgerDocument(int SchemaVersion, List<LedgerRecord> Records)
{
    public static LedgerDocument Read(string path)
    {
        var ledger = JsonSerializer.Deserialize<LedgerDocument>(File.ReadAllText(path), Artifacts.JsonOptions)
            ?? throw new InvalidDataException("Ledger is empty");
        if (ledger.SchemaVersion != 1 || ledger.Records is null)
        {
            throw new InvalidDataException("Unsupported ledger schema");
        }
        return ledger;
    }
}

public sealed class KafkaScenario : IDisposable
{
    public const string BootstrapServers = "127.0.0.1:9092";
    private readonly IAdminClient _admin;
    private readonly IProducer<Null, byte[]> _producer;

    public KafkaScenario(string topic, string group)
    {
        Topic = topic;
        Group = group;
        var adminConfig = new AdminClientConfig { BootstrapServers = BootstrapServers };
        adminConfig.Set("log_level", "0");
        _admin = new AdminClientBuilder(adminConfig).Build();
        var producerConfig = new ProducerConfig { BootstrapServers = BootstrapServers };
        producerConfig.Set("log_level", "0");
        _producer = new ProducerBuilder<Null, byte[]>(producerConfig).Build();
    }

    public string Topic { get; }

    public string Group { get; }

    public Task CreateTopics() =>
        _admin.CreateTopicsAsync(
            [new TopicSpecification { Name = Topic, NumPartitions = 1, ReplicationFactor = 1 }],
            new CreateTopicsOptions { RequestTimeout = TimeSpan.FromSeconds(30) });

    private async Task<PublishedRecord> Publish(string payload, CancellationToken cancellationToken = default)
    {
        var bytes = Encoding.UTF8.GetBytes(payload);
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeout.CancelAfter(TimeSpan.FromSeconds(10));
        var delivered = await _producer.ProduceAsync(
            new TopicPartition(Topic, 0),
            new Message<Null, byte[]> { Value = bytes },
            timeout.Token);
        return new PublishedRecord(
            delivered.Topic,
            delivered.Partition.Value,
            delivered.Offset.Value,
            Convert.ToHexStringLower(SHA256.HashData(bytes)),
            payload);
    }

    public async Task<long> CommittedOffset()
    {
        var result = await _admin.ListConsumerGroupOffsetsAsync(
            [new ConsumerGroupTopicPartitions(Group, [new TopicPartition(Topic, 0)])],
            new ListConsumerGroupOffsetsOptions { RequestTimeout = TimeSpan.FromSeconds(5) });
        return result.Single().Partitions.Single().Offset.Value;
    }

    private async Task<long> LogEndOffset()
    {
        var partition = new TopicPartition(Topic, 0);
        var result = await _admin.ListOffsetsAsync(
            [new TopicPartitionOffsetSpec { TopicPartition = partition, OffsetSpec = OffsetSpec.Latest() }],
            new ListOffsetsOptions { RequestTimeout = TimeSpan.FromSeconds(5) });
        var offset = result.ResultInfos.Single().TopicPartitionOffsetError;
        if (offset.Error.IsError)
        {
            throw new KafkaException(offset.Error);
        }
        return offset.Offset.Value;
    }

    public async Task<(PublishedRecord Baseline, PublishedRecord Poison, PublishedRecord Tail)> InjectIncident(
        string runDirectory,
        string ledgerPath,
        string failureLog,
        CancellationToken cancellationToken)
    {
        var baseline = await Publish("{\"productId\":\"P-baseline\",\"productType\":\"Physical\",\"price\":100}", cancellationToken);
        await Wait(
            "baseline processing and commit",
            async () => File.Exists(ledgerPath) && LedgerDocument.Read(ledgerPath).Records.Count == 1 &&
                        await CommittedOffset() == baseline.Offset + 1,
            cancellationToken: cancellationToken);
        var poison = await Publish("{\"productId\":\"P-poison\",\"productType\":null,\"price\":100}", cancellationToken);
        var tail = await Publish("{\"productId\":\"P-tail\",\"productType\":\"Digital\",\"price\":50}", cancellationToken);
        Artifacts.Phase(runDirectory, "injected");
        await Wait(
            "three poison retries",
            () => Task.FromResult(CountFailures(failureLog) >= 3),
            TimeSpan.FromSeconds(15),
            cancellationToken);
        var committed = await CommittedOffset();
        if (LedgerDocument.Read(ledgerPath).Records.Count != 1 || committed != poison.Offset)
        {
            throw new InvalidOperationException($"Consumer was not blocked at poison offset {poison.Offset}: commit={committed}");
        }
        return (baseline, poison, tail);
    }

    public async Task<(PublishedRecord Probe, Dictionary<string, bool> Checks)> VerifyRecovery(
        string ledgerPath,
        PublishedRecord baseline,
        PublishedRecord poison,
        PublishedRecord tail,
        CancellationToken cancellationToken)
    {
        await Wait(
            "poison rejection and tail recovery",
            async () => LedgerDocument.Read(ledgerPath).Records.Count == 3 && await CommittedOffset() == tail.Offset + 1,
            cancellationToken: cancellationToken);
        var probeId = $"P-probe-{Guid.NewGuid():N}";
        var probe = await Publish(JsonSerializer.Serialize(new { productId = probeId, productType = "Physical", price = 75 }), cancellationToken);
        await Wait(
            "fresh probe processing",
            async () => LedgerDocument.Read(ledgerPath).Records.Count == 4 && await CommittedOffset() == probe.Offset + 1,
            cancellationToken: cancellationToken);

        var records = LedgerDocument.Read(ledgerPath).Records.ToDictionary(record => record.Offset);
        var expectedOffsets = new[] { baseline.Offset, poison.Offset, tail.Offset, probe.Offset }.ToHashSet();
        var checks = new Dictionary<string, bool>
        {
            ["baseline_preserved"] = Matches(records[baseline.Offset], baseline.PayloadSha256, "processed", "P-baseline", "physical", 100, null),
            ["poison_rejected"] = Matches(records[poison.Offset], poison.PayloadSha256, "rejected", "P-poison", null, null, null),
            ["tail_processed"] = Matches(records[tail.Offset], tail.PayloadSha256, "processed", "P-tail", "digital", 50, null),
            ["probe_processed"] = Matches(records[probe.Offset], probe.PayloadSha256, "processed", probeId, "physical", 75, null),
            ["no_extra_records"] = records.Keys.ToHashSet().SetEquals(expectedOffsets),
            ["partition_drained"] = await CommittedOffset() == probe.Offset + 1 && await LogEndOffset() == probe.Offset + 1,
        };
        if (checks.Values.Any(value => !value))
        {
            throw new InvalidOperationException($"Recovery verification failed: {JsonSerializer.Serialize(checks)}");
        }
        return (probe, checks);
    }

    public void Dispose()
    {
        _producer.Dispose();
        _admin.Dispose();
    }

    private static bool Matches(
        LedgerRecord record,
        string hash,
        string disposition,
        string productId,
        string? normalizedType,
        decimal? price,
        string? reasonCode) =>
        record.PayloadSha256 == hash &&
        record.Disposition == disposition &&
        record.ProductId == productId &&
        (normalizedType is null || record.NormalizedType == normalizedType) &&
        (price is null || record.Price == price) &&
        (reasonCode is null || record.ReasonCode == reasonCode);

    private static int CountFailures(string path) => File.ReadAllText(path).Split("product.processing.failed").Length - 1;

    private static async Task Wait(
        string description,
        Func<Task<bool>> condition,
        TimeSpan? timeout = null,
        CancellationToken cancellationToken = default)
    {
        var timer = System.Diagnostics.Stopwatch.StartNew();
        var limit = timeout ?? TimeSpan.FromSeconds(30);
        while (timer.Elapsed < limit)
        {
            if (await condition())
            {
                return;
            }
            await Task.Delay(TimeSpan.FromMilliseconds(250), cancellationToken);
        }
        throw new TimeoutException($"Timed out waiting for {description}");
    }
}
