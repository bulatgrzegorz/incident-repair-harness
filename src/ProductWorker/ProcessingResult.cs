namespace ProductWorker;

public sealed record ProcessingResult(
    string Disposition,
    string ProductId,
    string? NormalizedType = null,
    decimal? Price = null,
    string? ReasonCode = null)
{
    public static ProcessingResult Processed(string productId, string normalizedType, decimal price) =>
        new("processed", productId, normalizedType, price);

    public static ProcessingResult Rejected(string productId, string reasonCode) =>
        new("rejected", productId, ReasonCode: reasonCode);
}
