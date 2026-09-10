using System.Text.Json;

namespace ProductWorker;

public static class ProductProcessor
{
    public static ProcessingResult Process(ReadOnlySpan<byte> payload)
    {
        using var document = JsonDocument.Parse(payload.ToArray());
        var root = document.RootElement;

        var productId = root.TryGetProperty("productId", out var idElement)
            ? idElement.GetString() ?? ""
            : "";

        if (string.IsNullOrWhiteSpace(productId))
        {
            return ProcessingResult.Rejected(productId, "missing_product_id");
        }

        if (!root.TryGetProperty("price", out var priceElement) ||
            !priceElement.TryGetDecimal(out var price) ||
            price < 0)
        {
            return ProcessingResult.Rejected(productId, "invalid_price");
        }

        string? productType = root.TryGetProperty("productType", out var typeElement)
            ? typeElement.GetString()
            : null;

        var normalizedType = productType!.Trim().ToLowerInvariant();

        return normalizedType is "physical" or "digital"
            ? ProcessingResult.Processed(productId, normalizedType, price)
            : ProcessingResult.Rejected(productId, "unsupported_product_type");
    }
}
