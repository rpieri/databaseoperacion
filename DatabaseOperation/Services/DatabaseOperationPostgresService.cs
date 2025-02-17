using DatabaseOperation.Interfaces;
using DatabaseOperation.Model;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Npgsql;
using System.Data;
using System.Text;

namespace DatabaseOperation.Services
{
    public class DatabaseOperationPostgresService : IDatabase
    {
        private readonly ILogger<DatabaseOperationPostgresService> _ilogger;
        private readonly string _connectionString;

        public DatabaseOperationPostgresService(ILogger<DatabaseOperationPostgresService> ilogger, IConfiguration configuration)
        {
            _ilogger = ilogger;
            _connectionString = configuration["DatabaseSettings:ConnectionStringPostgres"];
        }

        public IEnumerable<int> Execute(IEnumerable<TableData> listOfTableProperties)
        {
            List<int> results = new();

            foreach (var property in listOfTableProperties)
                results.Add(Execute(property));

            return results;
        }

        public int Execute(TableData properties)
        {
            return properties.enumOperation switch
            {
                EnumOperation.Create => Create(properties.schema, properties.TableName, properties),
                EnumOperation.Update => Update(properties.schema, properties.TableName, properties),
                EnumOperation.Delete => Delete(properties.schema, properties.TableName, properties),
                _ => 0,
            };
        }
        private int Create(string schema, string tableName, TableData properties)
        {
            using (NpgsqlConnection connection = new(_connectionString))
            {
                connection.Open();

                using var tran = connection.BeginTransaction();
                try
                {
                    var tempTableName = CreateAndFillTempTable(schema, tableName, properties, connection, tran);

                    int rowAffected = 0;

                    var sql = GenerateSQLQueryInsert(schema, tableName, tempTableName);

                    using (NpgsqlCommand cmd = new NpgsqlCommand(sql, connection, tran))
                    {
                        rowAffected = cmd.ExecuteNonQuery();
                    }

                    tran.Commit();

                    return rowAffected;

                }
                catch (Exception)
                {
                    tran.Rollback();
                }
            }
            return 0;
        }
        private int Delete(string schema, string tableName, TableData properties)
        {
            using (NpgsqlConnection connection = new(_connectionString))
            {
                connection.Open();

                using NpgsqlTransaction tran = connection.BeginTransaction();
                try
                {
                    var tempTableName = CreateAndFillTempTable(schema, tableName, properties, connection, tran);

                    int rowAffected = 0;

                    var sql = GenerateSQLQueryDelete(schema, tableName, tempTableName, properties.properties);

                    using (NpgsqlCommand cmd = new(sql, connection, tran))
                    {
                        rowAffected = cmd.ExecuteNonQuery();
                    }

                    tran.Commit();

                    return rowAffected;

                }
                catch (Exception)
                {
                    tran.Rollback();
                }
            }
            return 0;
        }
        private int Update(string schema, string tableName, TableData properties)
        {
            using (NpgsqlConnection connection = new NpgsqlConnection(_connectionString))
            {
                connection.Open();

                using NpgsqlTransaction tran = connection.BeginTransaction();

                try
                {
                    var tempTableName = CreateAndFillTempTable(schema, tableName, properties, connection, tran);

                    var sql = GenerateSQLQueryUpdate(schema, tableName, tempTableName, properties.properties);

                    int rowAffected = 0;

                    using (var cmd = new NpgsqlCommand(sql, connection, tran))
                    {
                        rowAffected = cmd.ExecuteNonQuery();
                    }

                    tran.Commit();

                    return rowAffected;

                }
                catch (Exception)
                {
                    tran.Rollback();
                }
            }
            return 0;
        }
        private string GenerateSQLQueryInsert(string schema, string table, string tempTable)
        {
            var sql = $"INSERT INTO {schema}.{table} SELECT * FROM {tempTable} ";
            return sql;
        }
        private string GenerateSQLQueryUpdate(string schema, string table, string tempTableName, IEnumerable<TableProperty> properties)
        {
            StringBuilder sql = new();
            sql.Append($"UPDATE {schema}.{table} T SET ");

            var setAssignments = properties
                .Select(property => $" {property.Name} = s.{property.Name}")
                .ToList();

            sql.Append(string.Join(", ", setAssignments));

            sql.Append($" FROM {tempTableName} S WHERE 1=1 AND ");

            var primaryKeyConditions = properties
                .Where(p => p.IsPrimaryKey)
                .Select(p => $" T.{p.Name} = S.{p.Name} ")
                .ToList();

            for (int i = 0; i < primaryKeyConditions.Count; i++)
            {
                if (i < primaryKeyConditions.Count - 1)
                {
                    sql.Append(primaryKeyConditions[i] + " AND ");
                }
                else
                {
                    sql.Append(primaryKeyConditions[i]);
                }
            }

            return sql.ToString();
        }

        private string GenerateSQLQueryDelete(string schema, string table, string tempTable, IEnumerable<TableProperty> properties)
        {
            StringBuilder sql = new();
            sql.AppendLine($"DELETE FROM {schema}.{table} c USING {tempTable} S WHERE 1=1 AND ");

            var conditions = properties
                            .Where(p => p.IsPrimaryKey)
                            .Select(p => $" C.{p.Name} = S.{p.Name} ");

            sql.AppendLine(string.Join(" AND ", conditions));

            return sql.ToString();
        }
        private void CreateTemporaryTable(string schema, string tempTable, string originalTable, NpgsqlConnection con, NpgsqlTransaction tran)
        {
            using NpgsqlCommand cmd = new NpgsqlCommand($"CREATE TEMP TABLE {tempTable} ON COMMIT DROP AS SELECT * FROM {schema}.{originalTable} LIMIT 0", con, tran);
            cmd.ExecuteNonQuery();
        }
        private void BulkIntoTempTable(string schema, string tempTableName, TableData tableData, NpgsqlConnection connection, NpgsqlTransaction tran)
        {
            try
            {
                StringBuilder copySql = new();
                copySql.Append($"COPY {tempTableName} (");
                copySql.Append(string.Join(",", tableData.properties.Select(field => field.Name)));
                copySql.Append(") FROM STDIN (FORMAT BINARY)");

                using (var writer = connection.BeginBinaryImport(copySql.ToString()))
                {
                    foreach (var row in tableData.Data)
                    {
                        writer.StartRow();

                        foreach (var column in row)
                        {
                            writer.Write(column.Value);
                        }
                    }

                    writer.Complete();
                }
            }
            catch (Exception)
            {
                throw;
            }
        }
        private string CreateAndFillTempTable(string schema, string tableName, TableData properties, NpgsqlConnection connection, NpgsqlTransaction tran)
        {
            var newTempTableName = GenerateNameTempTable(tableName);

            CreateTemporaryTable(schema, newTempTableName, tableName, connection, tran);

            BulkIntoTempTable(schema, newTempTableName, properties, connection, tran);

            return newTempTableName;
        }

        protected string GenerateNameTempTable(string tableName)
        {
            if (string.IsNullOrEmpty(tableName))
            {
                throw new ArgumentException("Table name is empty.", tableName);
            }

            var compl = DateTime.Now.Ticks;
            return $"{tableName}__{compl}";
        }
    }
}
