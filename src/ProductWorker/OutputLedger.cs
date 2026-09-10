using System.Text.Json;
using System.Text.Json.Serialization;

namespace ProductWorker;

public sealed record LedgerRecord(
    string Topic,
    int Partition,
    long Offset,
    string PayloadSha256,
    string Disposition,
    string ProductId,
    string? NormalizedType = null,
    decimal? Price = null,
    string? ReasonCode = null);

public sealed record LedgerDocument(int SchemaVersion, List<LedgerRecord> Records);

public sealed class OutputLedger
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.SnakeCaseLower,
        WriteIndented = true,
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
    };

    private readonly string _path;
    private readonly LedgerDocument _document;

    public OutputLedger(string outputDirectory)
    {
        Directory.CreateDirectory(outputDirectory);
        _path = Path.Combine(outputDirectory, "processed-products.json");
        _document = File.Exists(_path)
            ? JsonSerializer.Deserialize<LedgerDocument>(File.ReadAllText(_path), JsonOptions)
                ?? throw new InvalidDataException("Ledger is empty.")
            : new LedgerDocument(1, []);

        if (_document.SchemaVersion != 1 || _document.Records is null)
        {
            throw new InvalidDataException("Unsupported ledger schema.");
        }

        if (_document.Records.GroupBy(record => (record.Topic, record.Partition, record.Offset)).Any(group => group.Count() > 1))
        {
            throw new InvalidDataException("Ledger contains duplicate Kafka coordinates.");
        }
    }

    public bool Contains(string topic, int partition, long offset, string payloadHash)
    {
        var existing = _document.Records.SingleOrDefault(record =>
            record.Topic == topic && record.Partition == partition && record.Offset == offset);

        if (existing is not null && existing.PayloadSha256 != payloadHash)
        {
            throw new InvalidDataException("Ledger payload hash conflicts with the Kafka record.");
        }

        return existing is not null;
    }

    public void Append(string topic, int partition, long offset, string payloadHash, ProcessingResult result)
    {
        _document.Records.Add(new LedgerRecord(
            topic,
            partition,
            offset,
            payloadHash,
            result.Disposition,
            result.ProductId,
            result.NormalizedType,
            result.Price,
            result.ReasonCode));

        var temporaryPath = $"{_path}.{Guid.NewGuid():N}.tmp";
        try
        {
            using (var stream = new FileStream(temporaryPath, FileMode.CreateNew, FileAccess.Write, FileShare.None))
            {
                JsonSerializer.Serialize(stream, _document, JsonOptions);
                stream.Flush(flushToDisk: true);
            }

            File.Move(temporaryPath, _path, overwrite: true);
        }
        finally
        {
            File.Delete(temporaryPath);
        }
    }
}
