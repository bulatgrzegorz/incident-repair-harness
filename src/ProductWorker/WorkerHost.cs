using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace ProductWorker;

public static class WorkerHost
{
    public static IHost Create(WorkerSettings settings)
    {
        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddSingleton(settings);
        builder.Services.AddHostedService<KafkaWorker>();
        return builder.Build();
    }
}
