using System.Security.Cryptography;
using System.Text.Json;
using Confluent.Kafka;
using Microsoft.Extensions.Hosting;

namespace ProductWorker;

public sealed class KafkaWorker(WorkerSettings settings) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await Task.Yield();
        Run(settings, stoppingToken);
    }

    private static void Run(WorkerSettings settings, CancellationToken cancellationToken)
    {
        var ledger = new OutputLedger(settings.OutputDirectory);
        using var telemetry = new WorkerTelemetry(settings);
        var configuration = new ConsumerConfig
        {
            BootstrapServers = settings.BootstrapServers,
            GroupId = settings.GroupId,
            EnableAutoCommit = false,
            EnableAutoOffsetStore = false,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            AllowAutoCreateTopics = false,
        };

        using var consumer = new ConsumerBuilder<Ignore, byte[]>(configuration).Build();
        consumer.Subscribe(settings.Topic);

        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                telemetry.Heartbeat();
                var record = consumer.Consume(TimeSpan.FromSeconds(1));
                if (record is null)
                {
                    continue;
                }

                var payload = record.Message.Value ?? [];
                var hash = Convert.ToHexStringLower(SHA256.HashData(payload));
                if (ledger.Contains(record.Topic, record.Partition.Value, record.Offset.Value, hash))
                {
                    consumer.Commit(record);
                    continue;
                }

                ProcessingResult result;
                try
                {
                    using var activity = telemetry.StartProcessing(record);
                    result = ProductProcessor.Process(payload);
                }
                catch (Exception exception) when (exception is JsonException or InvalidOperationException or NullReferenceException)
                {
                    Console.Error.WriteLine(telemetry.Failed(record, exception));
                    consumer.Seek(record.TopicPartitionOffset);
                    telemetry.ObserveBroker(consumer, record.TopicPartition);
                    Task.Delay(TimeSpan.FromSeconds(1), cancellationToken).GetAwaiter().GetResult();
                    continue;
                }

                ledger.Append(record.Topic, record.Partition.Value, record.Offset.Value, hash, result);
                consumer.Commit(record);
                telemetry.Completed(result);
                telemetry.ObserveBroker(consumer, record.TopicPartition);
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
        }
        finally
        {
            consumer.Close();
        }
    }
}
