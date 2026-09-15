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
            await process.WaitForExitAsync(CancellationToken.None);
            await Task.WhenAll(stdout, stderr);
            cancellationToken.ThrowIfCancellationRequested();
            throw new TimeoutException($"{executable} timed out after {timeout.TotalSeconds:0} seconds");
        }

        var result = new CommandResult(process.ExitCode, await stdout, await stderr);
        if (logPath is not null)
        {
            await File.AppendAllTextAsync(logPath, result.StandardOutput + result.StandardError, cancellation.Token);
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
            if (result.Length <= limit) continue;
            
            result.Remove(0, result.Length - limit);
            truncated = true;
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
    private readonly Process _process;
    private readonly FileStream _stdout;
    private readonly FileStream _stderr;
    private readonly Task _stdoutCopy;
    private readonly Task _stderrCopy;

    public bool HasExited => _process.HasExited;

    private WorkerProcess(Process process, FileStream stdout, FileStream stderr)
    {
        _process = process;
        _stdout = stdout;
        _stderr = stderr;
        _stdoutCopy = process.StandardOutput.BaseStream.CopyToAsync(stdout);
        _stderrCopy = process.StandardError.BaseStream.CopyToAsync(stderr);
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
            if (!_process.HasExited)
            {
                try
                {
                    UnixHost.Interrupt(_process.Id);
                }
                catch (InvalidOperationException) when (_process.HasExited)
                {
                }
                using var cancellation = new CancellationTokenSource(TimeSpan.FromSeconds(10));
                try
                {
                    await _process.WaitForExitAsync(cancellation.Token);
                }
                catch (OperationCanceledException)
                {
                    _process.Kill(entireProcessTree: true);
                    await _process.WaitForExitAsync(CancellationToken.None);
                }
            }
            await Task.WhenAll(_stdoutCopy, _stderrCopy);
        }
        finally
        {
            if (!_process.HasExited)
            {
                _process.Kill(entireProcessTree: true);
                await _process.WaitForExitAsync();
            }
            await _stdout.DisposeAsync();
            await _stderr.DisposeAsync();
            _process.Dispose();
        }
    }
}
