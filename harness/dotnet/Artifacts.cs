using System.Security.Cryptography;
using System.Text.Encodings.Web;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.RegularExpressions;
using Spectre.Console;

namespace IncidentHarness;

public static partial class Artifacts
{
    public static readonly JsonSerializerOptions JsonOptions = new()
    {
        WriteIndented = true,
        PropertyNamingPolicy = JsonNamingPolicy.SnakeCaseLower,
        Encoder = JavaScriptEncoder.UnsafeRelaxedJsonEscaping,
    };

    public static void WriteJson(string path, object? value)
    {
        var temporary = path + ".tmp";
        File.WriteAllText(temporary, JsonSerializer.Serialize(value, JsonOptions) + "\n");
        File.Move(temporary, path, overwrite: true);
    }

    public static void Phase(string runDirectory, string name) =>
        File.AppendAllText(
            Path.Combine(runDirectory, "events.jsonl"),
            JsonSerializer.Serialize(new { phase = name, occurred_at = DateTimeOffset.UtcNow.ToString("O") }) + "\n");

    public static void FinalizeSuccess(string runDirectory, string mode)
    {
        _ = SafeFiles(runDirectory).ToArray();
        Phase(runDirectory, "finalized");
        WriteJson(Path.Combine(runDirectory, "verdict.json"), new
        {
            schema_version = 1,
            outcome = "resolved",
            mode,
            report_status = "not_requested",
            experiment_complete = false,
            reason = "Repair verified; model-authored post-mortem is not implemented yet.",
        });

        var entries = SafeFiles(runDirectory)
            .Where(path => Path.GetFileName(path) != "manifest.json" && !HasDirectory(path, "bin") && !HasDirectory(path, "obj"))
            .Order(StringComparer.Ordinal)
            .Select(path =>
            {
                var content = File.ReadAllBytes(path);
                return new
                {
                    path = Path.GetRelativePath(runDirectory, path).Replace(Path.DirectorySeparatorChar, '/'),
                    size = content.Length,
                    sha256 = Convert.ToHexStringLower(SHA256.HashData(content)),
                };
            })
            .ToArray();
        WriteJson(Path.Combine(runDirectory, "manifest.json"), new { schema_version = 1, artifacts = entries });
    }

    public static int Inspect(string root, string runId)
    {
        if (!RunId().IsMatch(runId))
        {
            throw new ArgumentException("Invalid run ID");
        }
        var verdict = Path.Combine(root, "runs", runId, "verdict.json");
        var manifest = Path.Combine(root, "runs", runId, "manifest.json");
        if (!File.Exists(verdict) || !File.Exists(manifest))
        {
            throw new FileNotFoundException($"No finalized verdict for {runId}");
        }
        AnsiConsole.Console.Profile.Out.Writer.Write(File.ReadAllText(verdict));
        return 0;
    }

    public static JsonNode ReadJson(string path) => JsonNode.Parse(File.ReadAllText(path)) ?? throw new JsonException($"Empty JSON file: {path}");

    private static bool HasDirectory(string path, string name) =>
        path.Split(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar).Contains(name, StringComparer.Ordinal);

    private static IEnumerable<string> SafeFiles(string root)
    {
        var pending = new Stack<string>();
        pending.Push(root);
        while (pending.TryPop(out var directory))
        {
            foreach (var path in Directory.EnumerateFileSystemEntries(directory))
            {
                var attributes = File.GetAttributes(path);
                if ((attributes & FileAttributes.ReparsePoint) != 0)
                {
                    throw new InvalidOperationException($"Run artifacts contain a symlink: {Path.GetRelativePath(root, path)}");
                }
                if ((attributes & FileAttributes.Directory) != 0)
                {
                    pending.Push(path);
                }
                else
                {
                    yield return path;
                }
            }
        }
    }

    [GeneratedRegex(@"^\d{8}T\d{6}Z-[a-f0-9]{8}$")]
    private static partial Regex RunId();
}
