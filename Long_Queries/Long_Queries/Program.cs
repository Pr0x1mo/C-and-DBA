using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using ClosedXML.Excel;
using Oracle.DataAccess.Client;

namespace Long_Queries
{
    internal class Program
    {
        private static XLWorkbook workbook = new XLWorkbook();
        private static readonly object workbookLock = new object();
        private static string excelFilePath = Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.Desktop), "Top 10 Queries.xlsx");
        private static readonly Dictionary<string, string> connectionStrings = new Dictionary<string, string>
        {     
            {"ASTPRD", "Data Source=(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)(HOST=10.1.1.213)(PORT=1521)))(CONNECT_DATA=(SERVER=DEDICATED)(SERVICE_NAME=astprd_pdb1.prodprisubphx.consortiexpxvcn.oraclevcn.com)));User Id=xborja;Password=Seraphim24$"}
            //{"ASTDEV", "Data Source=(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)(HOST=10.0.2.11)(PORT=1521)))(CONNECT_DATA=(SERVER=DEDICATED)(SERVICE_NAME=astdev_pdb1.nonprodprisub.consortiexvcn.oraclevcn.com)));User Id=xborja;Password=Seraphim24$"},    
            //{"ASTDEMO", "Data Source=(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)(HOST=10.0.2.15)(PORT=1521)))(CONNECT_DATA=(SERVER=DEDICATED)(SERVICE_NAME=astdemo_pdb1.nonprodprisub.consortiexvcn.oraclevcn.com)));User Id=xborja;Password=Seraphim24$"},
            //{"ASTSIT", "Data Source=(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)(HOST=10.0.2.14)(PORT=1521)))(CONNECT_DATA=(SERVER=DEDICATED)(SERVICE_NAME=astsit_pdb1.nonprodprisub.consortiexvcn.oraclevcn.com)));User Id=xborja;Password=Seraphim24$"}
        };
        //static void Main(string[] args)
        //{

        //}

        static async Task Main()
        {
            Stopwatch stopwatch = new Stopwatch();
            stopwatch.Start();

            await CallQueriesAsync();

            stopwatch.Stop();

            string formattedTime;

            TimeSpan elapsed = stopwatch.Elapsed;

            if (elapsed.TotalSeconds >= 60)
            {
                if (elapsed.TotalMinutes >= 60)
                {
                    // If elapsed time is over 60 minutes, show hours, minutes, and seconds
                    formattedTime = $"{(int)elapsed.TotalHours} hours, {(int)elapsed.TotalMinutes % 60} minutes, {elapsed.Seconds} seconds";
                }
                else
                {
                    // If elapsed time is over 60 seconds, show minutes and seconds
                    formattedTime = $"{(int)elapsed.TotalMinutes} minutes, {elapsed.Seconds} seconds";
                }
            }
            else
            {
                // Otherwise, show seconds
                formattedTime = $"{elapsed.TotalSeconds} seconds";
            }

            Console.WriteLine("Total execution time: " + formattedTime);
        }

        static async Task CallQueriesAsync()
        {          
                await queries();           
        }

        static async Task queries()
        {
            var task0 = ZeroUsedIDXs();
            var task1 = leastUsedIDXs();
            var task2 = topTenLongQueries();
            var task3 = topTenQueries();
            await Task.WhenAll(task0, task1, task2, task3);
            //await Task.WhenAll(task3);
            var uploader = new SharePointFileUploader(excelFilePath, "/Documents/");
            
            
        }

        static async Task ZeroUsedIDXs()
        {


            string query = @"WITH AllIndexes AS (
                                                    SELECT
                                                        i.owner,
                                                        i.index_name,
                                                        c.constraint_type
                                                    FROM
                                                        dba_indexes i
                                                    LEFT JOIN
                                                        dba_constraints c ON i.table_owner = c.owner AND i.table_name = c.table_name AND c.index_name = i.index_name
                                                    WHERE
                                                         i.owner IN ('ASSURTRK', 'ASSURTRKVOR')
                                                         AND (c.constraint_type IS NULL OR c.constraint_type NOT IN ('P', 'R'))
                                                    )
                                                    SELECT
                                                    ai.owner,
                                                    ai.index_name,
                                                    ai.constraint_type,
                                                    COALESCE(num_executions, 0) AS num_executions
                                                    FROM
                                                    AllIndexes ai
                                                    LEFT JOIN (
                                                    SELECT
                                                        i.owner,
                                                        i.index_name,
                                                        c.constraint_type,
                                                        COUNT(*) AS num_executions
                                                    FROM
                                                        dba_hist_sqlstat s
                                                    JOIN
                                                        dba_hist_sql_plan p ON s.sql_id = p.sql_id AND s.plan_hash_value = p.plan_hash_value
                                                    JOIN
                                                        dba_indexes i ON p.object_owner = i.owner AND p.object_name = i.index_name
                                                    JOIN
                                                        dba_hist_snapshot sp ON sp.snap_id = s.snap_id
                                                    LEFT JOIN
                                                        dba_constraints c ON i.table_owner = c.owner AND i.table_name = c.table_name AND c.index_name = i.index_name

                                                    GROUP BY
                                                        i.owner, i.index_name, c.constraint_type
                                                    ) q ON ai.owner = q.owner AND ai.index_name = q.index_name AND     ai.constraint_type = q.constraint_type
                                                    where num_executions is null
                                                    ORDER BY
                                                    index_name ASC";

            

            lock (workbookLock)
            {
                foreach (var dumbdbName in connectionStrings.Keys)
                {
                    string connectionString = connectionStrings[dumbdbName];
                    var dbName = dumbdbName.Replace("AST", "");
                    var worksheet = workbook.Worksheets.Add(dbName + " IDXs not used");
                    Console.WriteLine("I am in " + dbName);
                    // Code goes here
                    using (OracleConnection connection = new OracleConnection(connectionString))
                    {
                        connection.Open();
                        

                        using (OracleCommand command = new OracleCommand(query, connection))
                        using (OracleDataReader reader = command.ExecuteReader())
                        {
                            int row = 1;
                            // Insert headers into the first row
                            for (int columnIndex = 1; columnIndex <= reader.FieldCount; columnIndex++)
                            {
                                worksheet.Cell(row, columnIndex).Value = reader.GetName(columnIndex - 1);
                            }

                            if (reader.HasRows)
                            {
                                while (reader.Read())
                                {
                                    row++;
                                    object[] values = new object[reader.FieldCount];
                                    reader.GetValues(values);
                                    string rowValues = string.Join(", ", values);
                                    Console.WriteLine(rowValues);
                                    worksheet.Cell(row, 1).Value = reader["OWNER"].ToString();
                                    worksheet.Cell(row, 2).Value = reader["index_name"].ToString();
                                    worksheet.Cell(row, 3).Value = reader["CONSTRAINT_TYPE"].ToString();
                                    worksheet.Cell(row, 4).Value = 0; //reader["num_executions"].ToString();
                                }
                            }
                            else
                            {
                                Console.WriteLine(dbName + " has no data");
                            }

                            worksheet.Columns().AdjustToContents();
                        }
                    }
                }
                workbook.SaveAs(excelFilePath);
            }
         
        }
        static async Task leastUsedIDXs()
        {
            

            string query = @"SELECT
                            i.owner,
                            i.index_name,
                            COUNT(*) AS num_executions
                        FROM
                            dba_hist_sqlstat s
                            JOIN dba_hist_sql_plan p ON s.sql_id = p.sql_id AND s.plan_hash_value = p.plan_hash_value
                            JOIN dba_indexes i ON p.object_owner = i.owner AND p.object_name = i.index_name
                            join dba_hist_snapshot sp on sp.snap_id = s.snap_id
                            LEFT JOIN dba_constraints c ON i.table_owner = c.owner AND i.table_name = c.table_name AND c.index_name = i.index_name
                        WHERE
                            s.elapsed_time_total > 0
                            and i.owner IN ('ASSURTRK', 'ASSURTRKVOR')
                            AND sp.BEGIN_INTERVAL_TIME >= SYSDATE - 30
                            AND (c.constraint_type IS NULL OR c.constraint_type NOT IN ('P', 'R'))
                        GROUP BY
                            i.owner, i.index_name
                        ORDER BY
                            num_executions asc
                        FETCH FIRST 100 ROWS ONLY";

            string query2 = @"SELECT
                            i.owner,
                            i.index_name,
                            COUNT(*) AS num_executions
                        FROM
                            v$sql v
                            JOIN dba_indexes i ON v.parsing_schema_name = i.owner
                            LEFT JOIN dba_constraints c ON i.table_owner = c.owner AND i.table_name = c.table_name and c.index_name = i.index_name
                        WHERE
                            v.elapsed_time > 0
                            and i.owner IN ('ASSURTRK', 'ASSURTRKVOR')
                            AND last_active_time >= SYSDATE - 30
                            AND (
                                c.constraint_type IS NULL
                                OR (c.constraint_type NOT IN ('P', 'R'))
                            )
                        GROUP BY
                            i.owner, i.index_name
                        ORDER BY
                            num_executions ASC
                        FETCH FIRST 100 ROWS ONLY";

            lock (workbookLock)
            {
                foreach (var dumbdbName in connectionStrings.Keys)
                {
                    string connectionString = connectionStrings[dumbdbName];
                    var dbName = dumbdbName.Replace("AST", "");
                    var worksheet = workbook.Worksheets.Add(dbName + " Top IDXs not used");
                    Console.WriteLine("I am in " + dbName);
                    // Code goes here
                    using (OracleConnection connection = new OracleConnection(connectionString))
                    {
                        connection.Open();
                        string queryToUse = dbName == "PRD" ? query : query2;

                        using (OracleCommand command = new OracleCommand(queryToUse, connection))
                        using (OracleDataReader reader = command.ExecuteReader())
                        {
                            int row = 1;
                            // Insert headers into the first row
                            for (int columnIndex = 1; columnIndex <= reader.FieldCount; columnIndex++)
                            {
                                worksheet.Cell(row, columnIndex).Value = reader.GetName(columnIndex - 1);
                            }

                            if (reader.HasRows)
                            {
                                while (reader.Read())
                                {
                                    row++;
                                    object[] values = new object[reader.FieldCount];
                                    reader.GetValues(values);
                                    string rowValues = string.Join(", ", values);
                                    Console.WriteLine(rowValues);
                                    worksheet.Cell(row, 1).Value = reader["OWNER"].ToString();
                                    worksheet.Cell(row, 2).Value = reader["index_name"].ToString();
                                    worksheet.Cell(row, 3).Value = reader["num_executions"].ToString();
                                }
                            }
                            else
                            {
                                Console.WriteLine(dbName + " has no data");
                            }

                            worksheet.Columns().AdjustToContents();
                        }
                    }
                }
                workbook.SaveAs(excelFilePath);
            }
            // topTenLongQueries();
        }

        static async Task topTenLongQueries()
        {
            

            string query = @"select 
                            sq.sql_text,
                            dhss.sql_id,
                            ROUND((elapsed_time_total / 1000000) / 60, 2) as total_elapsed_seconds
                            from dba_hist_sqlstat dhss,
                            dba_hist_snapshot sn, 
                            dba_hist_sqltext sq 
                            where dhss.snap_id = sn.snap_id and sq.sql_id = dhss.sql_id
                            and sn.begin_interval_time >= TRUNC(SYSDATE - 7)
                            AND sn.begin_interval_time < TRUNC(SYSDATE)
                            and PARSING_SCHEMA_NAME  in ('ASSURTRK_USER', 'ASSURTRKVOR_USER')
                            order by total_elapsed_seconds desc nulls last";

            string query2 = @"Select /*+ parallel(8) */
                            v.sql_text,
                            v.sql_id,
                            round((v.elapsed_time/1000000),2) as total_elapsed_seconds
                        From v$sql v
                        where  last_active_time >= SYSDATE - 7
                        and v.parsing_schema_name in ('ASSURTRK_USER', 'ASSURTRKVOR_USER')
                        ORDER BY
                            total_elapsed_seconds DESC";

            lock (workbookLock)
            {
                foreach (var dumbdbName in connectionStrings.Keys)
                {
                    string connectionString = connectionStrings[dumbdbName];
                    var dbName = dumbdbName.Replace("AST", "");
                    var worksheet = workbook.Worksheets.Add(dbName + " Top10LongQueries");
                    using (OracleConnection connection = new OracleConnection(connectionString))
                    {
                        var dataTable = new DataTable();
                        connection.Open();
                        string queryToUse = dbName == "PRD" ? query : query2;

                        using (OracleCommand command = new OracleCommand(queryToUse, connection))
                        using (OracleDataAdapter adapter = new OracleDataAdapter(command))
                        {
                            adapter.Fill(dataTable);
                        }

                        if (dataTable.Rows.Count > 0)
                        {
                            var groupedData = from row in dataTable.AsEnumerable()
                                              group row by new
                                              {
                                                  sqlText = row.Field<string>("sql_text"),
                                                  sqlId = row.Field<string>("sql_id")
                                              }
                                              into Group
                                              select new
                                              {
                                                  SqlText = Group.Key.sqlText,
                                                  SqlId = Group.Key.sqlId,
                                                  TotalElapsedSeconds = Group.Sum(g => Convert.ToDouble(g.Field<decimal>("total_elapsed_seconds"))),
                                                  QueryCount = Group.Count()
                                              };

                            // Display the results before grouping
                            Console.WriteLine("I am in " + dbName + " and Results before grouping:");
                            foreach (var row in groupedData)
                            {
                                Console.WriteLine($"Query: {row.SqlText}, Module: {row.SqlId}, Total Elapsed Seconds: {row.TotalElapsedSeconds}, Query Count: {row.QueryCount}");
                            }

                            // Order the grouped data and select the top 10 queries
                            var topQueries = groupedData.OrderByDescending(item => item.TotalElapsedSeconds).Take(10);
                            var topQueriesList = topQueries.ToList();

                            int columnIndex = 1;
                            foreach (var propertyInfo in topQueriesList[0].GetType().GetProperties())
                            {
                                worksheet.Cell(1, columnIndex).Value = propertyInfo.Name;
                                columnIndex++;
                            }

                            int rowIndex = 2;
                            foreach (var queryData in topQueriesList)
                            {
                                columnIndex = 1;
                                foreach (var propertyInfo in queryData.GetType().GetProperties())
                                {
                                    var value = propertyInfo.GetValue(queryData);
                                    worksheet.Cell(rowIndex, columnIndex).Value = value != null ? value.ToString() : "";
                                    columnIndex++;
                                }
                                rowIndex++;
                            }
                        }
                        else
                        {
                            Console.WriteLine("No top ten longest query data for " + dbName + " database.");
                        }

                        worksheet.Columns().AdjustToContents();
                    }
                }
                workbook.SaveAs(excelFilePath);
            }
            // topTenQueries();
        }

        static async Task topTenQueries()
        {
            

            string query = @"SELECT
                            sq.sql_text,
                            dhss.sql_id
                            from dba_hist_sqlstat dhss,
                            dba_hist_snapshot sn, 
                            dba_hist_sqltext sq 
                            where dhss.snap_id = sn.snap_id and sq.sql_id = dhss.sql_id
                            and sn.begin_interval_time >= TRUNC(SYSDATE - 7)
                            AND sn.begin_interval_time < TRUNC(SYSDATE)
                            and PARSING_SCHEMA_NAME  in ('ASSURTRK_USER', 'ASSURTRKVOR_USER')
                            order by executions_total desc nulls last";

            string query2 = @"SELECT
                            v.sql_text,
                            v.sql_id
                        from v$sql v
                        where  last_active_time >= SYSDATE - 7
                        and v.parsing_schema_name in ('ASSURTRK_USER', 'ASSURTRKVOR_USER')
                        ORDER BY
                            executions desc nulls last";

            lock (workbookLock)
            {
                foreach (var dumbdbName in connectionStrings.Keys)
                {
                    string connectionString = connectionStrings[dumbdbName];
                    var dbName = dumbdbName.Replace("AST", "");
                    var worksheet = workbook.Worksheets.Add(dbName + " Top 10 Queries");
                    using (OracleConnection connection = new OracleConnection(connectionString))
                    {
                        var dataTable = new DataTable();
                        connection.Open();
                        string queryToUse = dbName == "PRD" ? query : query2;

                        using (OracleCommand command = new OracleCommand(queryToUse, connection))
                        using (OracleDataAdapter adapter = new OracleDataAdapter(command))
                        {
                            adapter.Fill(dataTable);
                        }

                        if (dataTable.Rows.Count > 0)
                        {
                            var groupedData = from row in dataTable.AsEnumerable()
                                              group row by new
                                              {
                                                  sqlText = row.Field<string>("sql_text"),
                                                  sqlId = row.Field<string>("sql_id")
                                              }
                                              into Group
                                              select new
                                              {
                                                  SqlText = Group.Key.sqlText,
                                                  SqlId = Group.Key.sqlId,
                                                  QueryCount = Group.Count() // Count how many times the sql_text occurs
                                              };

                            // Display the results before grouping
                            Console.WriteLine("I am in " + dbName + " and Results before grouping:");
                            foreach (var row in groupedData)
                            {
                                Console.WriteLine($"Query: {row.SqlText}, sqlid: {row.SqlId}, Query Count: {row.QueryCount}");
                            }

                            // Order the grouped data and select the top 10 queries
                            var topQueries = groupedData.OrderByDescending(item => item.QueryCount).Take(10);
                            var topQueriesList = topQueries.ToList();

                            int columnIndex = 1;
                            foreach (var propertyInfo in topQueriesList[0].GetType().GetProperties())
                            {
                                worksheet.Cell(1, columnIndex).Value = propertyInfo.Name;
                                columnIndex++;
                            }

                            int rowIndex = 2;
                            foreach (var queryData in topQueriesList)
                            {
                                columnIndex = 1;
                                foreach (var propertyInfo in queryData.GetType().GetProperties())
                                {
                                    var value = propertyInfo.GetValue(queryData);
                                    worksheet.Cell(rowIndex, columnIndex).Value = value != null ? value.ToString() : "";
                                    columnIndex++;
                                }
                                rowIndex++;
                            }
                        }
                        else
                        {
                            Console.WriteLine("No top ten most used query data for " + dbName + " database.");
                        }

                        worksheet.Columns().AdjustToContents();
                    }
                }
                workbook.SaveAs(excelFilePath);
            }
            // Open the saved Excel workbook
            Process.Start(excelFilePath);
        }

   

    }
}

