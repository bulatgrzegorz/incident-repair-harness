using System.Diagnostics;
using System.Net;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;

namespace IncidentHarness;

public static class GrafanaEvidence
{
    internal const string AlertName = "Product worker processing failures with consumer lag";

    internal static JsonNode? FindAlert(JsonArray alerts) => alerts.FirstOrDefault(item =>
        item?["labels"]?["alertname"]?.GetValue<string>() == AlertName);

    public static async Task<JsonNode> WaitForAlert(
        string outputDirectory,
        TimeSpan? timeout = null,
        CancellationToken cancellationToken = default)
    {
        Directory.CreateDirectory(outputDirectory);
        var limit = timeout ?? TimeSpan.FromSeconds(90);
        var deadline = Stopwatch.GetTimestamp() + (long)(limit.TotalSeconds * Stopwatch.Frequency);
        JsonNode? latest = null;
        var observations = Path.Combine(outputDirectory, "alert-observations.jsonl");
        using var handler = new HttpClientHandler { UseProxy = false };
        using var client = new HttpClient(handler);
        client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue(
            "Basic", Convert.ToBase64String(Encoding.ASCII.GetBytes("admin:admin")));

        while (Remaining(deadline) > TimeSpan.Zero)
        {
            var observation = new JsonObject { ["observed_at"] = DateTimeOffset.UtcNow.ToString("O") };
            try
            {
                using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
                cancellation.CancelAfter(Min(TimeSpan.FromSeconds(5), Remaining(deadline)));
                using var response = await client.GetAsync(
                    "http://127.0.0.1:3000/api/alertmanager/grafana/api/v2/alerts?active=true",
                    cancellation.Token);
                observation["status_code"] = (int)response.StatusCode;
                response.EnsureSuccessStatusCode();
                latest = JsonNode.Parse(await response.Content.ReadAsStringAsync(cancellation.Token));
                observation["response"] = latest?.DeepClone();
                await AppendObservation(observations, observation);
                if (latest is JsonArray alerts && FindAlert(alerts) is { } alert)
                {
                    var result = new JsonObject
                    {
                        ["schema_version"] = 1,
                        ["source"] = "grafana",
                        ["alert"] = alert.DeepClone(),
                    };
                    Artifacts.WriteJson(Path.Combine(outputDirectory, "alert.json"), result);
                    return result;
                }
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception exception) when (exception is HttpRequestException or TaskCanceledException or JsonException)
            {
                observation["error"] = $"{exception.GetType().Name}: {exception.Message}";
                await AppendObservation(observations, observation);
            }

            var remaining = Remaining(deadline);
            if (remaining > TimeSpan.Zero)
            {
                await Task.Delay(Min(TimeSpan.FromSeconds(2), remaining), cancellationToken);
            }
        }

        Artifacts.WriteJson(Path.Combine(outputDirectory, "alert-timeout.json"), new JsonObject { ["last_response"] = latest?.DeepClone() });
        throw new TimeoutException("Grafana alert did not fire");
    }

    private static TimeSpan Remaining(long deadline) => Stopwatch.GetElapsedTime(Stopwatch.GetTimestamp(), deadline);

    private static TimeSpan Min(TimeSpan left, TimeSpan right) => left <= right ? left : right;

    private static Task AppendObservation(string path, JsonObject observation) =>
        File.AppendAllTextAsync(path, observation.ToJsonString() + "\n");
}
