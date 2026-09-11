using System.Diagnostics;
using System.Text;

namespace IncidentHarness;

public sealed record CommandResult(int ExitCode, string StandardOutput, string StandardError);

public static class ProcessRunner
{
    public static async Task<CommandResult> Run(
        string executable,
        IEnumerable<string> arguments,
        TimeSpan timeout,
        string? workingDirectory = null,
        IReadOnlyDictionary<string, string>? environment = null,
        bool check = true,
        string? logPath = null,
        CancellationToken cancellationToken = default)
    {
        var startInfo = new ProcessStartInfo(executable)
        {
            WorkingDirectory = workingDirectory ?? Environment.CurrentDirectory,
            RedirectStandardInput = true,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
        };
        foreach (var argument in arguments)
        {
            startInfo.ArgumentList.Add(argument);
        }
        if (environment is not null)
        {
            foreach (var (name, value) in environment)
            {
                startInfo.Environment[name] = value;
            }
        }

        using var process = Process.Start(startInfo) ?? throw new InvalidOperationException($"Could not start {executable}");
        process.StandardInput.Close();
        var stdout = ReadTail(process.StandardOutput);
        var stderr = ReadTail(process.StandardError);
        using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        cancellation.CancelAfter(timeout);
        try
        {
            await process.WaitForExitAsync(cancellation.Token);
        }
        catch (OperationCanceledException) when (cancellation.IsCancellationRequested)
        {
            process.Kill(entireProcessTree: true);
            await process.WaitForExitAsync();
            await Task.WhenAll(stdout, stderr);
            cancellationToken.ThrowIfCancellationRequested();
            throw new TimeoutException($"{executable} timed out after {timeout.TotalSeconds:0} seconds");
        }

        var result = new CommandResult(process.ExitCode, await stdout, await stderr);
        if (logPath is not null)
        {
            await File.AppendAllTextAsync(logPath, result.StandardOutput + result.StandardError);
        }
        if (check && result.ExitCode != 0)
        {
            throw new InvalidOperationException($"{executable} exited with {result.ExitCode}: {(result.StandardError + result.StandardOutput).Trim()}");
        }

        return result;
    }

    public static async Task<bool> Works(string executable, CancellationToken cancellationToken = default, params string[] arguments)
    {
        try
        {
            return (await Run(executable, arguments, TimeSpan.FromSeconds(10), check: false, cancellationToken: cancellationToken)).ExitCode == 0;
        }
        catch (Exception exception) when (exception is IOException or System.ComponentModel.Win32Exception or TimeoutException)
        {
            return false;
        }
    }

    private static async Task<string> ReadTail(StreamReader reader)
    {
        const int limit = 1_048_576;
        var result = new StringBuilder();
        var buffer = new char[8192];
        var truncated = false;
        int read;
        while ((read = await reader.ReadAsync(buffer)) > 0)
        {
            result.Append(buffer, 0, read);
            if (result.Length > limit)
            {
                result.Remove(0, result.Length - limit);
                truncated = true;
            }
        }
        return truncated ? "[earlier output truncated]\n" + result : result.ToString();
    }

    public static async Task<int> RunToFiles(
        string executable,
        IEnumerable<string> arguments,
        TimeSpan timeout,
        string stdoutPath,
        string stderrPath,
        string? workingDirectory = null,
        CancellationToken cancellationToken = default)
    {
        var startInfo = new ProcessStartInfo(executable)
        {
            WorkingDirectory = workingDirectory ?? Environment.CurrentDirectory,
            RedirectStandardInput = true,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
        };
        foreach (var argument in arguments)
        {
            startInfo.ArgumentList.Add(argument);
        }

        await using var stdout = new FileStream(stdoutPath, FileMode.Create, FileAccess.Write, FileShare.Read);
        await using var stderr = new FileStream(stderrPath, FileMode.Create, FileAccess.Write, FileShare.Read);
        using var process = Process.Start(startInfo) ?? throw new InvalidOperationException($"Could not start {executable}");
        process.StandardInput.Close();
        var copies = Task.WhenAll(
            process.StandardOutput.BaseStream.CopyToAsync(stdout),
            process.StandardError.BaseStream.CopyToAsync(stderr));
        using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        cancellation.CancelAfter(timeout);
        try
        {
            await process.WaitForExitAsync(cancellation.Token);
        }
        catch (OperationCanceledException) when (cancellation.IsCancellationRequested)
        {
            process.Kill(entireProcessTree: true);
            await process.WaitForExitAsync();
            await copies;
            cancellationToken.ThrowIfCancellationRequested();
            throw new TimeoutException($"{executable} timed out after {timeout.TotalSeconds:0} seconds");
        }
        await copies;
        return process.ExitCode;
    }
}

public sealed class WorkerProcess : IAsyncDisposable
{
    private readonly Process process;
    private readonly FileStream stdout;
    private readonly FileStream stderr;
    private readonly Task stdoutCopy;
    private readonly Task stderrCopy;

    public bool HasExited => process.HasExited;

    private WorkerProcess(Process process, FileStream stdout, FileStream stderr)
    {
        this.process = process;
        this.stdout = stdout;
        this.stderr = stderr;
        stdoutCopy = process.StandardOutput.BaseStream.CopyToAsync(stdout);
        stderrCopy = process.StandardError.BaseStream.CopyToAsync(stderr);
    }

    public static WorkerProcess Start(string workerDll, IReadOnlyDictionary<string, string> environment, string stdoutPath, string stderrPath)
    {
        var startInfo = new ProcessStartInfo("dotnet")
        {
            RedirectStandardInput = true,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
        };
        startInfo.ArgumentList.Add(workerDll);
        foreach (var (name, value) in environment)
        {
            startInfo.Environment[name] = value;
        }

        FileStream? stdout = null;
        FileStream? stderr = null;
        Process? process = null;
        try
        {
            stdout = new FileStream(stdoutPath, FileMode.Append, FileAccess.Write, FileShare.ReadWrite);
            stderr = new FileStream(stderrPath, FileMode.Append, FileAccess.Write, FileShare.ReadWrite);
            process = Process.Start(startInfo) ?? throw new InvalidOperationException("Could not start ProductWorker");
            process.StandardInput.Close();
            return new WorkerProcess(process, stdout, stderr);
        }
        catch
        {
            if (process is { HasExited: false })
            {
                process.Kill(entireProcessTree: true);
            }
            process?.Dispose();
            stdout?.Dispose();
            stderr?.Dispose();
            throw;
        }
    }

    public async ValueTask DisposeAsync()
    {
        try
        {
            if (!process.HasExited)
            {
                try
                {
                    UnixHost.Interrupt(process.Id);
                }
                catch (InvalidOperationException) when (process.HasExited)
                {
                }
                using var cancellation = new CancellationTokenSource(TimeSpan.FromSeconds(10));
                try
                {
                    await process.WaitForExitAsync(cancellation.Token);
                }
                catch (OperationCanceledException)
                {
                    process.Kill(entireProcessTree: true);
                    await process.WaitForExitAsync();
                }
            }
            await Task.WhenAll(stdoutCopy, stderrCopy);
        }
        finally
        {
            if (!process.HasExited)
            {
                process.Kill(entireProcessTree: true);
                await process.WaitForExitAsync();
            }
            await stdout.DisposeAsync();
            await stderr.DisposeAsync();
            process.Dispose();
        }
    }
}
