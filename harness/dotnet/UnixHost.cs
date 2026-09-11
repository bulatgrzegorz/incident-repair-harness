using System.Diagnostics;
using System.Globalization;
using System.Runtime.InteropServices;

namespace IncidentHarness;

public static class UnixHost
{
    public static string User => $"{GetUserId()}:{GetGroupId()}";

    public static void EnsureSupported()
    {
        if (!OperatingSystem.IsLinux() && !OperatingSystem.IsMacOS())
        {
            throw new PlatformNotSupportedException("The C# harness currently supports macOS and Linux");
        }
    }

    public static void Interrupt(int processId)
    {
        EnsureSupported();
        if (kill(processId, 2) != 0)
        {
            throw new InvalidOperationException($"Could not interrupt process {processId}");
        }
    }

    public static int HardLinkCount(string path)
    {
        EnsureSupported();
        var arguments = OperatingSystem.IsMacOS() ? new[] { "-f", "%l", path } : new[] { "-c", "%h", path };
        var startInfo = new ProcessStartInfo("stat")
        {
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
        };
        foreach (var argument in arguments)
        {
            startInfo.ArgumentList.Add(argument);
        }
        using var process = Process.Start(startInfo) ?? throw new InvalidOperationException("Could not start stat");
        var output = process.StandardOutput.ReadToEnd();
        process.WaitForExit();
        if (process.ExitCode != 0 || !int.TryParse(output.Trim(), NumberStyles.None, CultureInfo.InvariantCulture, out var count))
        {
            throw new IOException($"Could not inspect hard links for {path}: {process.StandardError.ReadToEnd().Trim()}");
        }

        return count;
    }

    [DllImport("libc")]
    private static extern uint getuid();

    [DllImport("libc")]
    private static extern uint getgid();

    [DllImport("libc", SetLastError = true)]
    private static extern int kill(int pid, int signal);

    private static uint GetUserId()
    {
        EnsureSupported();
        return getuid();
    }

    private static uint GetGroupId()
    {
        EnsureSupported();
        return getgid();
    }
}
