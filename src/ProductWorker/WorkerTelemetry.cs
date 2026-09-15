using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Text.Json;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using OpenTelemetry;
using OpenTelemetry.Logs;
using OpenTelemetry.Metrics;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;

namespace ProductWorker;

public sealed class WorkerTelemetry : IDisposable
{
    private const string MeterName = "ProductWorker";
    private const string ActivitySourceName = "ProductWorker";
    private readonly ActivitySource _activitySource = new(ActivitySourceName);
    private readonly KeyValuePair<string, object?>[] _tags;
    private readonly Meter _meter = new(MeterName);
    private readonly MeterProvider _meterProvider;
    private readonly TracerProvider _tracerProvider;
    private readonly ILoggerFactory _loggerFactory;
    private readonly ILogger _logger;
    private long _successes;
    private long _failures;
    private long _rejections;
    private long _heartbeat;
    private long _lastSuccess;
    private long _committedNext = -1;
    private long _logEnd = -1;
    private long _lag = -1;
    private long _brokerObserved;

    public WorkerTelemetry(WorkerSettings settings)
    {
        _tags =
        [
            new("service_namespace", settings.ServiceNamespace),
            new("service_instance_id", settings.InstanceId),
        ];
        var resource = ResourceBuilder.CreateDefault().AddService(
            serviceName: "product-worker",
            serviceNamespace: settings.ServiceNamespace,
            serviceInstanceId: settings.InstanceId);

        _meter.CreateObservableCounter("product_processing_successes", () => new Measurement<long>(Interlocked.Read(ref _successes), _tags));
        _meter.CreateObservableCounter("product_processing_failures", () => new Measurement<long>(Interlocked.Read(ref _failures), _tags));
        _meter.CreateObservableCounter("product_processing_rejections", () => new Measurement<long>(Interlocked.Read(ref _rejections), _tags));
        _meter.CreateObservableGauge("product_worker_heartbeat_unixtime_seconds", () => new Measurement<long>(Interlocked.Read(ref _heartbeat), _tags));
        _meter.CreateObservableGauge("product_last_success_unixtime_seconds", () => new Measurement<long>(Interlocked.Read(ref _lastSuccess), _tags));
        _meter.CreateObservableGauge("product_consumer_committed_next_offset", () => new Measurement<long>(Interlocked.Read(ref _committedNext), _tags));
        _meter.CreateObservableGauge("product_partition_log_end_offset", () => new Measurement<long>(Interlocked.Read(ref _logEnd), _tags));
        _meter.CreateObservableGauge("product_consumer_lag", () => new Measurement<long>(Interlocked.Read(ref _lag), _tags));
        _meter.CreateObservableGauge("product_broker_observed_unixtime_seconds", () => new Measurement<long>(Interlocked.Read(ref _brokerObserved), _tags));

        _meterProvider = Sdk.CreateMeterProviderBuilder()
            .SetResourceBuilder(resource)
            .AddMeter(MeterName)
            .AddOtlpExporter((options, reader) =>
            {
                if (settings.OtlpEndpoint is not null)
                {
                    options.Endpoint = settings.OtlpEndpoint;
                }
                reader.PeriodicExportingMetricReaderOptions.ExportIntervalMilliseconds = 1_000;
            })
            .Build();
        var tracing = Sdk.CreateTracerProviderBuilder()
            .SetResourceBuilder(resource)
            .AddSource(ActivitySourceName);
        _tracerProvider = settings.OtlpEndpoint is null
            ? tracing.AddOtlpExporter().Build()
            : tracing.AddOtlpExporter(options => options.Endpoint = settings.OtlpEndpoint).Build();
        _loggerFactory = LoggerFactory.Create(builder => builder.AddOpenTelemetry(options =>
        {
            options.SetResourceBuilder(resource);
            options.AddOtlpExporter(exporter =>
            {
                if (settings.OtlpEndpoint is not null)
                {
                    exporter.Endpoint = settings.OtlpEndpoint;
                }
            });
        }));
        _logger = _loggerFactory.CreateLogger("ProductWorker");
    }

    public void Heartbeat() => Interlocked.Exchange(ref _heartbeat, DateTimeOffset.UtcNow.ToUnixTimeSeconds());

    public Activity? StartProcessing(ConsumeResult<Ignore, byte[]> record)
    {
        var traceParent = Header(record.Message.Headers, "traceparent");
        var traceState = Header(record.Message.Headers, "tracestate");
        var parent = ActivityContext.TryParse(traceParent, traceState, out var context) ? context : default;
        var activity = _activitySource.StartActivity("process product", ActivityKind.Consumer, parent);
        activity?.SetTag("messaging.system", "kafka");
        activity?.SetTag("messaging.destination.name", record.Topic);
        activity?.SetTag("messaging.kafka.partition", record.Partition.Value);
        activity?.SetTag("messaging.kafka.offset", record.Offset.Value);
        return activity;
    }

    private static string? Header(Headers? headers, string name) =>
        headers?.TryGetLastBytes(name, out var value) == true && value is not null
            ? System.Text.Encoding.ASCII.GetString(value)
            : null;

    public void Completed(ProcessingResult result)
    {
        if (result.Disposition == "processed")
        {
            Interlocked.Increment(ref _successes);
            Interlocked.Exchange(ref _lastSuccess, DateTimeOffset.UtcNow.ToUnixTimeSeconds());
        }
        else
        {
            Interlocked.Increment(ref _rejections);
        }
    }

    public string Failed(ConsumeResult<Ignore, byte[]> record, Exception exception)
    {
        Interlocked.Increment(ref _failures);
        var body = JsonSerializer.Serialize(new
        {
            @event = "product.processing.failed",
            attempt_id = Guid.NewGuid(),
            service_instance_id = _tags[1].Value,
            topic = record.Topic,
            partition = record.Partition.Value,
            offset = record.Offset.Value,
            exception_type = exception.GetType().FullName,
            exception_message = exception.Message,
            stack_trace = exception.StackTrace,
        });
        _logger.LogError("{Failure}", body);
        return body;
    }

    public void RefreshBrokerMetrics(IConsumer<Ignore, byte[]> consumer, TopicPartition partition)
    {
        try
        {
            var committed = consumer.Committed([partition], TimeSpan.FromSeconds(2)).Single().Offset;
            var watermark = consumer.QueryWatermarkOffsets(partition, TimeSpan.FromSeconds(2));
            if (committed == Offset.Unset)
            {
                return;
            }

            Interlocked.Exchange(ref _committedNext, committed.Value);
            Interlocked.Exchange(ref _logEnd, watermark.High.Value);
            Interlocked.Exchange(ref _lag, watermark.High.Value - committed.Value);
            Interlocked.Exchange(ref _brokerObserved, DateTimeOffset.UtcNow.ToUnixTimeSeconds());
        }
        catch (KafkaException)
        {
            // Telemetry collection must not interrupt record processing.
        }
    }

    public void Dispose()
    {
        _loggerFactory.Dispose();
        _tracerProvider.Dispose();
        _meterProvider.Dispose();
        _activitySource.Dispose();
        _meter.Dispose();
    }
}
