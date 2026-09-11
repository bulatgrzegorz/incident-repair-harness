using Spectre.Console;

namespace IncidentHarness;

public static class ConsoleUi
{
    private static readonly IAnsiConsole ErrorConsole = AnsiConsole.Create(new AnsiConsoleSettings
    {
        Out = new AnsiConsoleOutput(Console.Error),
    });

    public static void Header(string runId, string mode, string runtime)
    {
        AnsiConsole.Write(new Rule("[bold orange3]INCIDENT REPAIR[/]").LeftJustified());
        AnsiConsole.MarkupLine($"[grey]{Markup.Escape(runId)}  |  {Markup.Escape(mode)}  |  {Markup.Escape(runtime)}[/]");
    }

    public static void Step(int current, int total, string message) =>
        AnsiConsole.MarkupLine($"[orange3]●[/] [grey]{current}/{total}[/]  {Markup.Escape(message)}");

    public static void StepDone(TimeSpan elapsed) =>
        AnsiConsole.MarkupLine($"   [green]✓[/] [grey]done in {elapsed.TotalSeconds:F1}s[/]");

    public static void Insight(string label, string message) =>
        AnsiConsole.MarkupLine($"   [orange3]◆[/] [bold]{Markup.Escape(label)}:[/] {Markup.Escape(message)}");

    public static Task<T> Status<T>(string message, Func<Task<T>> action) =>
        AnsiConsole.Status()
            .Spinner(Spinner.Known.Dots)
            .SpinnerStyle(Style.Parse("orange3"))
            .StartAsync(message, _ => action());

    public static void PreparationHeader(string agent, string runtime)
    {
        AnsiConsole.Write(new Rule("[bold orange3]PREPARE HARNESS[/]").LeftJustified());
        AnsiConsole.MarkupLine($"[grey]{Markup.Escape(agent)}  |  {Markup.Escape(runtime)}[/]");
    }

    public static void Prepared(IEnumerable<(string Image, string Purpose)> images, TimeSpan elapsed)
    {
        AnsiConsole.MarkupLine($"\n[bold green]✓ PREPARATION COMPLETE[/] [grey]({elapsed.TotalSeconds:F1}s)[/]");
        foreach (var (image, purpose) in images)
        {
            AnsiConsole.MarkupLine($"[green]ready[/]  {Markup.Escape(image)} [grey]({Markup.Escape(purpose)})[/]");
        }
    }

    public static void Success(string title, string message, string runDirectory)
    {
        AnsiConsole.MarkupLine($"\n[bold green]✓ {Markup.Escape(title)}[/]");
        AnsiConsole.MarkupLine(Markup.Escape(message));
        AnsiConsole.MarkupLine($"[grey]Evidence  {Markup.Escape(runDirectory)}[/]");
    }

    public static void Error(string message) =>
        ErrorConsole.MarkupLine($"[bold red]Error:[/] {Markup.Escape(message)}");

    public static void Checks(IEnumerable<(string Name, bool Available, string Detail)> checks)
    {
        var table = new Table().Border(TableBorder.Rounded).AddColumn("Status").AddColumn("Check").AddColumn("Detail");
        foreach (var (name, available, detail) in checks)
        {
            table.AddRow(
                available ? "[green]PASS[/]" : "[red]FAIL[/]",
                Markup.Escape(name),
                Markup.Escape(detail));
        }
        AnsiConsole.Write(table);
    }
}
