using DatabaseOperation.Interfaces;
using DatabaseOperation.Model;
using Microsoft.Extensions.Logging;
using System.Diagnostics;

namespace DatabaseOperation
{
    public interface IExecutionService
    {
        void Run();
    }

    public class ExecutionService : IExecutionService
    {
        private readonly IDatabase _database;
        private readonly ILogger _logger;

        public ExecutionService(ILogger<ExecutionService> logger, IDatabase database)
        {
            _database = database;
            _logger = logger;
        }

        public void Run()
        {
            var executionId = DateTime.Now.Ticks;

            using (_logger.BeginScope(new { ExecutionId = executionId }))
            {
                _logger.LogInformation("Iniciando a execução do serviço.");


                TableData tableData = new()
                {
                    schema = "poc",
                    TableName = "customer",
                    enumOperation = EnumOperation.Delete
                };

                tableData.properties.AddRange([new TableProperty
                {
                    Name = "ID",
                    IsPrimaryKey = true,
                },
                new TableProperty
                {
                    Name = "name",
                    IsPrimaryKey = false,
                },
                new TableProperty
                {
                    Name = "source",
                    IsPrimaryKey = false,
                },
                ]);

                for (int i = 500; i <= 100000; i++)
                {
                    var record = new List<KeyValuePair<string, dynamic>>
                    {
                        new("ID", i),
                        new("name", "Nome " + i),
                        new("source", "S")
                    };

                    tableData.Data.Add(record);
                }

                Stopwatch stopwatch = Stopwatch.StartNew();
                var totalExecuted = _database.Execute(tableData);
                stopwatch.Stop();

                _logger.LogInformation($"Total de registros {totalExecuted} - Time Elapsed: Tempo decorrido: {stopwatch.ElapsedMilliseconds}");
            }
        }
    }
}
