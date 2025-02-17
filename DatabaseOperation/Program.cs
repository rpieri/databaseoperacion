using DatabaseOperation;
using DatabaseOperation.Interfaces;
using DatabaseOperation.Services;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Console;
class Program
{
    static async Task Main(string[] args)
    {
        using IHost host = Host.CreateDefaultBuilder(args)
             .ConfigureAppConfiguration((context, config) =>
             {
                 config.AddJsonFile("appsettings.json", optional: false, reloadOnChange: true);
             })
            .ConfigureLogging(logging =>
            {
                logging.ClearProviders();

                logging.AddConsole(options =>
                {
                    options.FormatterName = ConsoleFormatterNames.Simple;
                });
            })
            .ConfigureServices((context, services) =>
            {
                services.AddTransient<IDatabase, DatabaseOperationSQLServerService>();
                services.AddTransient<IExecutionService, ExecutionService>();

                services.Configure<SimpleConsoleFormatterOptions>(options =>
                {
                    options.IncludeScopes = true;
                });
            })
            .Build();



        var myService = host.Services.GetRequiredService<IExecutionService>();
        myService.Run();
    }
}