using System.Text.Json.Nodes;
using IncidentHarness;

namespace IncidentHarness.Tests;

public class HarnessTests
{
    [Test]
    public async Task FinalizeCreatesVerdictAndManifestWithoutHashingManifestItself()
    {
        using var temporary = new TemporaryDirectory();
        await File.WriteAllTextAsync(Path.Combine(temporary.Path, "evidence.json"), "{}\n");

        Artifacts.FinalizeSuccess(temporary.Path, "fixture");

        var manifest = Artifacts.ReadJson(Path.Combine(temporary.Path, "manifest.json"));
        var paths = manifest["artifacts"]!.AsArray().Select(item => item!["path"]!.GetValue<string>()).ToArray();
        await Assert.That(paths).Contains("verdict.json");
        await Assert.That(paths).DoesNotContain("manifest.json");
    }

    [Test]
    public async Task AlertSelectionIgnoresGrafanaNoDataAlert()
    {
        var noData = new JsonObject
        {
            ["labels"] = new JsonObject { ["alertname"] = "DatasourceNoData", ["rulename"] = GrafanaEvidence.AlertName },
        };
        var firing = new JsonObject
        {
            ["labels"] = new JsonObject { ["alertname"] = GrafanaEvidence.AlertName },
        };

        var selected = GrafanaEvidence.FindAlert(new JsonArray(noData, firing));

        await Assert.That(selected).IsSameReferenceAs(firing);
    }

    [Test]
    public async Task CandidateManifestRejectsSymbolicAndHardLinks()
    {
        using var temporary = new TemporaryDirectory();
        var source = Path.Combine(temporary.Path, "source.cs");
        await File.WriteAllTextAsync(source, "source");
        var symbolic = Path.Combine(temporary.Path, "symbolic.cs");
        File.CreateSymbolicLink(symbolic, source);

        await AssertFails(() => CandidateWorkspace.Manifest(temporary.Path), "symlink");

        File.Delete(symbolic);
        var hard = Path.Combine(temporary.Path, "hard.cs");
        await ProcessRunner.Run("ln", [source, hard], TimeSpan.FromSeconds(5));
        await AssertFails(() => CandidateWorkspace.Manifest(temporary.Path), "unusual file");
    }

    [Test]
    public async Task BuildOutputCleanupLeavesSourceFiles()
    {
        using var temporary = new TemporaryDirectory();
        var bin = Path.Combine(temporary.Path, "project/bin");
        Directory.CreateDirectory(bin);
        await File.WriteAllTextAsync(Path.Combine(bin, "output.dll"), "compiled");
        var source = Path.Combine(temporary.Path, "source.cs");
        await File.WriteAllTextAsync(source, "source");

        CandidateWorkspace.CleanBuildOutputs(temporary.Path);

        await Assert.That(Directory.Exists(bin)).IsFalse();
        await Assert.That(File.Exists(source)).IsTrue();
    }

    [Test]
    public async Task FinalizeRejectsArtifactSymlinks()
    {
        using var temporary = new TemporaryDirectory();
        var target = Path.Combine(temporary.Path, "target.txt");
        await File.WriteAllTextAsync(target, "target");
        File.CreateSymbolicLink(Path.Combine(temporary.Path, "linked.txt"), target);

        await AssertFails(() => Artifacts.FinalizeSuccess(temporary.Path, "fixture"), "symlink");
        await Assert.That(File.Exists(Path.Combine(temporary.Path, "verdict.json"))).IsFalse();
    }

    [Test]
    public async Task LedgerUsesTypedSnakeCaseContract()
    {
        using var temporary = new TemporaryDirectory();
        var path = Path.Combine(temporary.Path, "processed-products.json");
        await File.WriteAllTextAsync(path, """
            {"schema_version":1,"records":[{"topic":"products","partition":0,"offset":2,"payload_sha256":"abc","disposition":"processed","product_id":"P-1","normalized_type":"physical","price":10}]}
            """);

        var ledger = LedgerDocument.Read(path);

        await Assert.That(ledger.Records[0].ProductId).IsEqualTo("P-1");
        await Assert.That(ledger.Records[0].Offset).IsEqualTo(2);
    }

    private static async Task AssertFails(Action action, string message)
    {
        Exception? observed = null;
        try
        {
            action();
        }
        catch (Exception exception)
        {
            observed = exception;
        }

        await Assert.That(observed).IsNotNull();
        await Assert.That(observed!.Message).Contains(message);
    }
}

public sealed class TemporaryDirectory : IDisposable
{
    public string Path { get; } = System.IO.Path.Combine(System.IO.Path.GetTempPath(), $"incident-harness-tests-{Guid.NewGuid():N}");

    public TemporaryDirectory() => Directory.CreateDirectory(Path);

    public void Dispose() => Directory.Delete(Path, recursive: true);
}
