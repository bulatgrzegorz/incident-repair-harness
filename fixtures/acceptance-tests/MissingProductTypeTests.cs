namespace ProductWorker.Tests;

public partial class ProductProcessingTests
{
    [Test]
    public async Task MissingProductTypeIsRejectedWithoutBlockingLaterInput()
    {
        var ledger = await RunScenario(
            """{"productId":"P-baseline","productType":"Digital","price":10}""",
            """{"productId":"P-missing","productType":null,"price":20}""",
            """{"productId":"P-tail","productType":"Physical","price":30}""");

        await Assert.That(ledger.Records[0].Disposition).IsEqualTo("processed");
        await Assert.That(ledger.Records[1].Disposition).IsEqualTo("rejected");
        await Assert.That(ledger.Records[1].ReasonCode).IsEqualTo("missing_product_type");
        await Assert.That(ledger.Records[2].Disposition).IsEqualTo("processed");
    }
}
