using DatabaseOperation.Interfaces;
using DatabaseOperation.Model;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System.Data;
using System.Text;

namespace DatabaseOperation.Services
{
    public class DatabaseOperationSQLServerService : IDatabase
    {
        private readonly ILogger<DatabaseOperationSQLServerService> _ilogger;
        private readonly string _connectionString;

        public DatabaseOperationSQLServerService(ILogger<DatabaseOperationSQLServerService> ilogger, IConfiguration configuration)
        {
            _ilogger = ilogger;
            _connectionString = configuration["DatabaseSettings:ConnectionStringSQLServer"];
        }

        public IEnumerable<int> Execute(IEnumerable<TableData> listOfTableProperties)
        {
            List<int> results = new ();

            foreach (var property in listOfTableProperties)
                results.Add(Execute(property));

            return results;
        }

        public int Execute(TableData properties)
        {
            return properties.enumOperation switch
            {
                EnumOperation.Create => Create(properties.TableName, properties),
                EnumOperation.Update => Update(properties.TableName, properties),
                EnumOperation.Delete => Delete(properties.TableName, properties),
                _ => 0,
            };
        }
        private int Create(string tableName, TableData properties)
        {
            using (SqlConnection connection = new(_connectionString))
            {
                connection.Open();

                using SqlTransaction tran = connection.BeginTransaction();
                try
                {
                    var tempTableName = CreateAndFillTempTable(tableName, properties, connection, tran);

                    int rowAffected = 0;

                    var sql = GenerateSQLQueryInsert(tableName, tempTableName);

                    using (SqlCommand cmd = new(sql, connection, tran))
                    {
                        rowAffected = cmd.ExecuteNonQuery();
                    }

                    tran.Commit();

                    _ilogger.LogInformation($"Total de registros adicionados {rowAffected}");

                    return rowAffected;

                }
                catch (Exception)
                {
                    tran.Rollback();
                }
            }
            return 0;
        }
        private int Delete(string tableName, TableData properties)
        {
            using (SqlConnection connection = new(_connectionString))
            {
                connection.Open();

                using SqlTransaction tran = connection.BeginTransaction();
                try
                {
                    var tempTableName = CreateAndFillTempTable(tableName, properties, connection, tran);

                    int rowAffected = 0;

                    var sql = GenerateSQLQueryDelete(tableName, tempTableName, properties.properties);

                    using (SqlCommand cmd = new(sql, connection, tran))
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
        private int Update(string tableName, TableData properties)
        {
            using (SqlConnection connection = new SqlConnection(_connectionString))
            {
                connection.Open();

                using SqlTransaction tran = connection.BeginTransaction();

                try
                {
                    var tempTableName = CreateAndFillTempTable(tableName, properties, connection, tran);

                    var sql = GenerateSQLQueryUpdate(tableName, tempTableName, properties.properties);

                    int rowAffected = 0;

                    using (SqlCommand cmd = new(sql, connection, tran))
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
        private string GenerateSQLQueryInsert(string table, string tempTable)
        {
            var sql = $"INSERT INTO {table} SELECT * FROM {tempTable} ";
            return sql;
        }
        private string GenerateSQLQueryUpdate(string table, string tempTableName, IEnumerable<TableProperty> properties)
        {
            StringBuilder sql = new();
            sql.AppendLine("UPDATE T SET ");

            var setAssignments = properties
                .Select(property => $"T.{property.Name} = s.{property.Name}")
                .ToList();

            sql.AppendLine(string.Join(",\n", setAssignments));

            sql.AppendLine($"FROM {table} T INNER JOIN {tempTableName} S ON");

            var primaryKeyConditions = properties
                .Where(p => p.IsPrimaryKey)
                .Select(p => $"T.{p.Name} = S.{p.Name}")
                .ToList();

            for (int i = 0; i < primaryKeyConditions.Count; i++)
            {
                if (i < primaryKeyConditions.Count - 1)
                {
                    sql.AppendLine(primaryKeyConditions[i] + " AND");
                }
                else
                {
                    sql.AppendLine(primaryKeyConditions[i]);
                }
            }

            return sql.ToString();
        }

        private string GenerateSQLQueryDelete(string table, string tempTable, IEnumerable<TableProperty> properties)
        {
            StringBuilder sql = new();
            sql.AppendLine($"DELETE C FROM {table} C JOIN {tempTable} S ON ");

            var conditions = properties
                            .Where(p => p.IsPrimaryKey)
                            .Select(p => $" C.{p.Name} = S.{p.Name} ");

            sql.AppendLine(string.Join(" AND ", conditions));

            return sql.ToString();
        }
        private void CreateTemporaryTable(string tempTable, string originalTable, SqlConnection con, SqlTransaction tran)
        {
            using SqlCommand cmd = new SqlCommand($"SELECT TOP 0 * INTO {tempTable} FROM {originalTable};", con, tran);
            cmd.ExecuteNonQuery();
        }
        private void BulkIntoTempTable(string tempTableName, TableData tableData, SqlConnection connection, SqlTransaction tran)
        {
            try
            {
                using (SqlBulkCopy bulkCopy = new(connection, SqlBulkCopyOptions.Default, tran))
                {
                    bulkCopy.DestinationTableName = tempTableName;

                    DataTable dataTable = new();

                    foreach (var property in tableData.properties)
                    {
                        dataTable.Columns.Add(property.Name);
                    }

                    foreach (var row in tableData.Data)
                    {
                        DataRow dtRow = dataTable.NewRow();

                        foreach (var column in row)
                        {
                            dtRow[column.Key] = column.Value;
                        }

                        dataTable.Rows.Add(dtRow);
                    }

                    bulkCopy.WriteToServer(dataTable);
                }
            }
            catch (Exception)
            {
                throw;
            }
        }
        private string CreateAndFillTempTable(string tableName, TableData properties, SqlConnection connection, SqlTransaction tran)
        {
            var newTempTableName = GenerateNameTempTable(tableName);

            CreateTemporaryTable(newTempTableName, tableName, connection, tran);

            BulkIntoTempTable(newTempTableName, properties, connection, tran);

            return newTempTableName;
        }

        private string GenerateNameTempTable(string tableName)
        {
            if (string.IsNullOrEmpty(tableName))
            {
                throw new ArgumentException("Table name is empty.", tableName);
            }

            var compl = DateTime.Now.Ticks;
            return $"#{tableName}__{compl}";
        }
    }
}
