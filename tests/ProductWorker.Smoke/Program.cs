using System.Text;
using ProductWorker;

static ProcessingResult Process(string json) => ProductProcessor.Process(Encoding.UTF8.GetBytes(json));

var physical = Process("""{"productId":"P-1","productType":"Physical","price":100}""");
var unsupported = Process("""{"productId":"P-2","productType":"Service","price":25.50}""");

if (physical != ProcessingResult.Processed("P-1", "physical", 100m))
{
    throw new Exception($"Unexpected physical result: {physical}");
}

if (unsupported != ProcessingResult.Rejected("P-2", "unsupported_product_type"))
{
    throw new Exception($"Unexpected unsupported result: {unsupported}");
}

var outputDirectory = Path.Combine(Path.GetTempPath(), $"product-worker-smoke-{Guid.NewGuid():N}");
try
{
    var ledger = new OutputLedger(outputDirectory);
    ledger.Append("products", 0, 4, new string('a', 64), physical);

    var reloaded = new OutputLedger(outputDirectory);
    if (!reloaded.Contains("products", 0, 4, new string('a', 64)))
    {
        throw new Exception("Persisted ledger record was not found after reload.");
    }

    try
    {
        reloaded.Contains("products", 0, 4, new string('b', 64));
        throw new Exception("Conflicting payload hash was accepted.");
    }
    catch (InvalidDataException)
    {
    }
}
finally
{
    Directory.Delete(outputDirectory, recursive: true);
}

Console.WriteLine("Product processor smoke checks passed.");
