using Oracle.DataAccess.Client;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading.Tasks;


namespace Toad_Emulator
{
    internal class Program
    {
        static string[] constraintType = { "P", "U", "R", "C" };
        static string outputFilePath = @"C:\Users\XavierBorja\desktop\schema_comparison.txt";

        static void Main()
        {
            

            Dictionary<string, string> connectionStrings = new Dictionary<string, string>
        {
            {"ASTPRD", "Data Source=(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)(HOST=10.1.1.213)(PORT=1521)))(CONNECT_DATA=(SERVER=DEDICATED)(SERVICE_NAME=astprd_pdb1.prodprisubphx.consortiexpxvcn.oraclevcn.com)));User Id=xborja;Password=Seraphim24$"},
            {"ASTDEV", "Data Source=(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)(HOST=10.0.2.11)(PORT=1521)))(CONNECT_DATA=(SERVER=DEDICATED)(SERVICE_NAME=astdev_pdb1.nonprodprisub.consortiexvcn.oraclevcn.com)));User Id=xborja;Password=Seraphim24$"},
            {"ASTDEMO", "Data Source=(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)(HOST=10.0.2.15)(PORT=1521)))(CONNECT_DATA=(SERVER=DEDICATED)(SERVICE_NAME=astdemo_pdb1.nonprodprisub.consortiexvcn.oraclevcn.com)));User Id=xborja;Password=Seraphim24$"},
            {"ASTSIT", "Data Source=(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)(HOST=10.0.2.14)(PORT=1521)))(CONNECT_DATA=(SERVER=DEDICATED)(SERVICE_NAME=astsit_pdb1.nonprodprisub.consortiexvcn.oraclevcn.com)));User Id=xborja;Password=Seraphim24$"}
            
        };

            string sourceKey = "ASTPRD";
            string targetKey = "ASTSIT";
            string SourceConnectionString = connectionStrings[sourceKey];
            string TargetConnectionString = connectionStrings[targetKey];

            Stopwatch stopwatch = new Stopwatch();

            // Start the stopwatch
            stopwatch.Start();
            using (OracleConnection sourceConnection = new OracleConnection(SourceConnectionString))
            {
                using (OracleConnection targetConnection = new OracleConnection(TargetConnectionString))
                {
                    sourceConnection.Open();
                    targetConnection.Open();

                    CompareSchemas(sourceConnection, targetConnection, sourceKey, targetKey);
                    
                    sourceConnection.Close();
                    targetConnection.Close();
                }
            }
            stopwatch.Stop();

            // Calculate elapsed time
            long elapsedTimeMilliseconds = stopwatch.ElapsedMilliseconds;
            TimeSpan timeSpan = TimeSpan.FromMilliseconds(elapsedTimeMilliseconds);

            string formattedElapsedTime = string.Format("{0:D2}:{1:D2}:{2:D2}.{3:D3}",
                                                        timeSpan.Hours,
                                                        timeSpan.Minutes,
                                                        timeSpan.Seconds,
                                                        timeSpan.Milliseconds);

            Console.WriteLine($"Total Execution Time: {formattedElapsedTime}");
        }

        public static void CompareSchemas(OracleConnection sourceConnection, OracleConnection targetConnection, string sourceName, string targetName)
        {
  
            using (StreamWriter outputFile = new StreamWriter(outputFilePath, false)) // false = overwrite file
            {
                // Parallel fetching of distinct owners
                Task<List<string>> sourceOwnersTask = Task.Run(() => GetDistinctOwners(sourceConnection, sourceName));
                Task<List<string>> targetOwnersTask = Task.Run(() => GetDistinctOwners(targetConnection, targetName));

                Task.WaitAll(sourceOwnersTask, targetOwnersTask);
                List<string> sourceOwners = sourceOwnersTask.Result;
                List<string> targetOwners = targetOwnersTask.Result;

                // Compare owners from both databases in a single pass
                CompareOwnersBothWays(sourceOwners, targetOwners, sourceConnection, targetConnection, outputFile, sourceName, targetName);

                // Get common owners and compare their tables
                List<string> commonOwners = sourceOwners.Intersect(targetOwners).ToList();

                Parallel.ForEach(commonOwners, (owner) =>
                {
                    CompareOwnerTables(owner, sourceConnection, targetConnection, outputFile, sourceName, targetName);
                });

                //RemoveDuplicateRowsAndSortFile(outputFilePath);

                Process.Start(outputFilePath);
            }
        }
        
        public static List<string> GetDistinctOwners(OracleConnection connection, string dbname)
        {
                string ownersQuery = @"
                SELECT DISTINCT username
                FROM all_users
                WHERE username NOT LIKE 'APEX_%'
                  AND username NOT LIKE 'ORD%'
                  and username not like 'LOFT%'
                  AND username NOT IN (
                      'SYS', 'SYSTEM', 'DBSNMP', 'APPQOSSYS', 'CTXSYS', 'ORDS_METADATA', 'OJVMSYS', 'LOFTXREF', 'FLOWS_FILES', 'GGADMIN',
                      'DVSYS', 'PERFSTAT', 'AUDSYS', 'GSMADMIN_INTERNAL', 'APEX_220100', 'OUTLN', 'XDB', 'WMSYS', 'DBSFWUSER',
                      'ORDSYS', 'MDSYS', 'OLAPSYS', 'LBACSYS', 'SNOMED_ADMIN',
                      'HR', 'SCOTT', 'OE', 'SH', 'NDC_ADMIN', 'RPT_ADMIN', 'FAAS_ADMIN', 'BHARTNETT', 'XBORJA', 'MSCHULTZ'
                  )
                ORDER BY username";
    
            List<string> owners = new List<string>();
     
            using (OracleCommand cmd = new OracleCommand(ownersQuery, connection))    
            {          
                using (OracleDataReader reader = cmd.ExecuteReader())          
                {              
                    while (reader.Read())              
                    {                  
                        string owner = reader.GetString(0);                  
                        owners.Add(owner);
                      //  Console.WriteLine($"{dbname} has these distinct owners: {owner}");
                
                    }
            
                }
        
            }
            return owners;
        }
        public static void CompareOwnersBothWays(List<string> sourceOwners, List<string> targetOwners, OracleConnection sourceConnection, OracleConnection targetConnection, StreamWriter outputFile, string sourceDatabaseName, string targetDatabaseName)
        {
            // Run the first comparison in a separate task
            Task firstComparisonTask = Task.Run(() =>
            {
                Parallel.ForEach(sourceOwners, (owner) =>
                {
                    if (!targetOwners.Contains(owner))
                    {
                        string errorMsg = $"Owner '{owner}' in {sourceDatabaseName} is missing in the {targetDatabaseName} Database.";
                        //Console.WriteLine(errorMsg);
                        lock (outputFile)
                        {
                            outputFile.WriteLine(errorMsg);
                        }
                    }
                });
            });

            // Run the second comparison in a separate task
            Task secondComparisonTask = Task.Run(() =>
            {
                Parallel.ForEach(targetOwners, (owner) =>
                {
                    if (!sourceOwners.Contains(owner))
                    {
                        string errorMsg = $"Owner '{owner}' in {targetDatabaseName} is missing in the {sourceDatabaseName} Database.";
                        //Console.WriteLine(errorMsg);
                        lock (outputFile)
                        {
                            outputFile.WriteLine(errorMsg);
                        }
                    }
                });
            });

            // Wait for both tasks to complete
            Task.WaitAll(firstComparisonTask, secondComparisonTask);
        }

        public static void CompareOwnerTables(string owner, OracleConnection sourceConnection, OracleConnection targetConnection, StreamWriter outputFile, string sourceDatabaseName, string targetDatabaseName)
        {
           // bool dataTypeDifferencesFound = false;
            Dictionary<string, List<string>> sourceTableMetaData = new Dictionary<string, List<string>>();
            Dictionary<string, List<string>> targetTableMetadata = new Dictionary<string, List<string>>();

            Task<List<string>> sourceTableNamesTask = Task.Run(() => GetTableNames(sourceConnection, owner, sourceDatabaseName));
            Task<List<string>> targetTableNamesTask = Task.Run(() => GetTableNames(targetConnection, owner, targetDatabaseName));

            Task.WaitAll(sourceTableNamesTask, targetTableNamesTask);
            List<string> sourceTableNames = sourceTableNamesTask.Result;
            List<string> targetTableNames = targetTableNamesTask.Result;

            // Compare missing tables
            CompareMissingTables(sourceTableNames, targetTableNames, owner, outputFile, sourceDatabaseName, targetDatabaseName);

            // Find common table names between source and target databases
            List<string> commonTableNames = sourceTableNames.Intersect(targetTableNames).ToList();

            // Parallel processing of common tables
            Parallel.ForEach(commonTableNames, (tableName) =>
            {
                ProcessTableComparisons(tableName, owner, sourceConnection, targetConnection, outputFile, sourceDatabaseName, targetDatabaseName); // sourceTableMetaData, targetTableMetadata
            });
        }

        public static List<string> GetTableNames(OracleConnection connection, string owner, string databasename)
        {
            string tablesQuery = $@"
                                    SELECT table_name 
                                    FROM all_tables 
                                    WHERE owner = '{owner}' 
                                    AND NOT (owner = 'ASSURTRK' AND table_name = 'TYPE')";
            List<string> tableNames = new List<string>();

            using (OracleCommand sourceTableCmd = new OracleCommand(tablesQuery, connection))
            {
                using (OracleDataReader sourceTableReader = sourceTableCmd.ExecuteReader())
                {
                    while (sourceTableReader.Read())
                    {
                        tableNames.Add(sourceTableReader.GetString(0));
                        //Console.WriteLine($"this is {databasename} Table names {owner}.{sourceTableReader.GetString(0)}");
                    }
                }
            }

            return tableNames;
        }

        public static void CompareMissingTables(List<string> sourceTableNames, List<string> targetTableNames, string owner, StreamWriter outputFile, string sourceDatabaseName, string targetDatabaseName)
        {
            // Tables missing in source that are present in target
            List<string> tablesMissingInSource = sourceTableNames.Except(targetTableNames).ToList();
            List<string> tablesMissingInTarget = targetTableNames.Except(sourceTableNames).ToList();

            // Parallel processing of missing tables in source
            Parallel.ForEach(tablesMissingInSource, (tableName) =>
            {
                string errorMsg = $"Table {owner}.{tableName} is missing in the {targetDatabaseName} that {sourceDatabaseName} has";
                lock (outputFile) // Ensure thread safety when writing to the file
                {
                    //Console.WriteLine(errorMsg);
                    outputFile.WriteLine(errorMsg);
                }
            });

            // Parallel processing of missing tables in target
            Parallel.ForEach(tablesMissingInTarget, (tableName) =>
            {
                string errorMsg = $"Table {owner}.{tableName} is missing in the {sourceDatabaseName} that {targetDatabaseName} has";
                lock (outputFile) // Ensure thread safety when writing to the file
                {
                    //Console.WriteLine(errorMsg);
                    outputFile.WriteLine(errorMsg);
                }
            });
        }

        public static void ProcessTableComparisons(string tableName, string owner, OracleConnection sourceConnection, OracleConnection targetConnection, StreamWriter outputFile, string sourceDatabaseName, string targetDatabaseName)
        {
            Task<List<string>> sourceColumnsTask = Task.Run(() => GetColumnsForTable(sourceConnection, owner, tableName));
            Task<List<string>> targetColumnsTask = Task.Run(() => GetColumnsForTable(targetConnection, owner, tableName));

            Task.WaitAll(sourceColumnsTask, targetColumnsTask);

            List<string> sourceColumns = sourceColumnsTask.Result;
            List<string> targetColumns = targetColumnsTask.Result;

            // Parallel processing for missing columns in target database
            Parallel.ForEach(sourceColumns, (columnName) =>
            {
                if (!targetColumns.Contains(columnName))
                {
                    string errorMsg = $"Column '{columnName}' is missing in the {targetDatabaseName} Database for table {owner}.{tableName} that {sourceDatabaseName} has.";
                    lock (outputFile) 
                    {
                        //Console.WriteLine(errorMsg);
                        outputFile.WriteLine(errorMsg);
                    }
                }
            });

            // Parallel processing for missing columns in source database
            Parallel.ForEach(targetColumns, (columnName) =>
            {
                if (!sourceColumns.Contains(columnName))
                {
                    string errorMsg = $"Column '{columnName}' is missing in the {sourceDatabaseName} Database for table {owner}.{tableName} that {targetDatabaseName} has.";
                    lock (outputFile) // Ensure thread safety when writing to the file
                    {
                        //Console.WriteLine(errorMsg);
                        outputFile.WriteLine(errorMsg);
                    }
                }
            });

            // Compare data types for common columns
            List<string> commonColumns = sourceColumns.Intersect(targetColumns).ToList();
            foreach (string columnName in commonColumns)
            {
                // Compare indexes for each common column
                CompareIndexes(owner, tableName, columnName, sourceConnection, targetConnection, outputFile, sourceDatabaseName, targetDatabaseName);

                // Compare data types for common columns
                Task<string> sourceDataTypeTask = Task.Run(() => GetColumnDataType(sourceConnection, owner, tableName, columnName));
                Task<string> targetDataTypeTask = Task.Run(() => GetColumnDataType(targetConnection, owner, tableName, columnName));

                Task.WaitAll(sourceDataTypeTask, targetDataTypeTask);

                string sourceDataType = sourceDataTypeTask.Result;
                string targetDataType = targetDataTypeTask.Result;

                if (!sourceDataType.Equals(targetDataType))
                {
                    string dataTypeErrorMsg = $"Data type mismatch in {targetDatabaseName} Database for column {columnName} in table {owner}.{tableName}: Source data type is {sourceDataType}, target data type is '{targetDataType}'.";
                    //Console.WriteLine(dataTypeErrorMsg);
                    outputFile.WriteLine(dataTypeErrorMsg);
                }
            }

            // Additional comparisons like indexes and constraints
            CompareConstraint(owner, tableName, sourceConnection, targetConnection, outputFile, sourceDatabaseName, targetDatabaseName);
           
        }

        public static List<string> GetColumnsForTable(OracleConnection connection, string owner, string tableName)
        {
            string columnsQuery = "SELECT column_name FROM all_tab_columns WHERE owner = :owner AND table_name = :tableName ORDER BY column_id";
            List<string> columns = new List<string>();

            try
            {
                using (OracleCommand command = new OracleCommand(columnsQuery, connection))
                {
                    // Using parameterized queries to prevent SQL Injection
                    command.Parameters.Add(new OracleParameter("owner", owner));
                    command.Parameters.Add(new OracleParameter("tableName", tableName));

                    using (OracleDataReader reader = command.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            columns.Add(reader.GetString(0));
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                // Handle or log the exception as needed
                Console.WriteLine($"Error fetching column names for {owner}.{tableName}: {ex.Message}");
            }

            return columns;
        }

        public static void CompareIndexes(string owner, string tableName, string columnName, OracleConnection sourceConnection, OracleConnection targetConnection, StreamWriter outputFile, string sourceDatabaseName, string targetDatabaseName)
        {
            string IndexesQuery = $@"
                                    SELECT ai.index_name, aic.column_name
                                    FROM all_indexes ai
                                    JOIN all_ind_columns aic ON ai.index_name = aic.index_name AND ai.table_name = aic.table_name AND ai.owner = aic.index_owner
                                    WHERE ai.owner = '{owner}' 
                                    AND ai.table_name = '{tableName}' 
                                    AND aic.column_name = '{columnName}'";
            

            Task<Dictionary<string, List<string>>> sourceIndexesTask = Task.Run(() => GetIndexes(IndexesQuery, sourceConnection));
            Task<Dictionary<string, List<string>>> targetIndexesTask = Task.Run(() => GetIndexes(IndexesQuery, targetConnection));

            Task.WaitAll(sourceIndexesTask, targetIndexesTask);

            Dictionary<string, List<string>> sourceIndexes = sourceIndexesTask.Result;
            Dictionary<string, List<string>> targetIndexes = targetIndexesTask.Result;



            // Parallel processing for missing indexes in target database
            Parallel.ForEach(sourceIndexes, (indexEntry) =>
            {
                if (!targetIndexes.ContainsKey(indexEntry.Key))
                {
                    string joinedColumns = String.Join(", ", indexEntry.Value);
                    string errorMsg = $"Index '{indexEntry.Key}' on columns [{joinedColumns}] is missing in the {targetDatabaseName} Database for table {owner}.{tableName} that {sourceDatabaseName} Database has.";
                    lock (outputFile) // Ensure thread safety when writing to the file
                    {
                        Console.WriteLine(errorMsg);
                        outputFile.WriteLine(errorMsg);
                    }
                }
            });

            // Parallel processing for missing indexes in source database
            Parallel.ForEach(targetIndexes, (indexEntry) =>
            {
                if (!sourceIndexes.ContainsKey(indexEntry.Key))
                {
                    string joinedColumns = String.Join(", ", indexEntry.Value);
                    string errorMsg = $"Index '{indexEntry.Key}' on columns [{joinedColumns}] is missing in the {targetDatabaseName} Database for table {owner}.{tableName} that {sourceDatabaseName} Database has.";
                    lock (outputFile)
                    {
                        Console.WriteLine(errorMsg);
                        outputFile.WriteLine(errorMsg);
                    }
                }
            });
        }

        private static Dictionary<string, List<string>> GetIndexes(string query, OracleConnection connection)
        {
            var indexes = new Dictionary<string, List<string>>();
            using (OracleCommand command = new OracleCommand(query, connection))
            {
                using (OracleDataReader reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        string indexName = reader.GetString(0);
                        string columnName = reader.GetString(1);

                        if (!indexes.ContainsKey(indexName))
                        {
                            indexes[indexName] = new List<string>();
                        }
                        indexes[indexName].Add(columnName);
                    }
                }
            }
            return indexes;
        }

        public static string GetColumnDataType(OracleConnection connection, string owner, string tableName, string columnName)
        {
            string dataTypeQuery = "SELECT data_type FROM all_tab_columns WHERE owner = :owner AND table_name = :tableName AND column_name = :columnName";
            string dataType = string.Empty;

            try
            {
                using (OracleCommand command = new OracleCommand(dataTypeQuery, connection))
                {
                    // Using parameterized queries to prevent SQL Injection
                    command.Parameters.Add(new OracleParameter("owner", owner));
                    command.Parameters.Add(new OracleParameter("tableName", tableName));
                    command.Parameters.Add(new OracleParameter("columnName", columnName));

                    dataType = Convert.ToString(command.ExecuteScalar());
                }
            }
            catch (Exception ex)
            {
                // Handle or log the exception as needed
                Console.WriteLine($"Error fetching data type for {owner}.{tableName}.{columnName}: {ex.Message}");
            }

            return dataType;
        }

        public static void CompareConstraint(string owner, string tableName, OracleConnection sourceConnection, OracleConnection targetConnection, StreamWriter outputFile, string sourceDatabaseName, string targetDatabaseName)
        {
            foreach (var constraint in constraintType)
            {
                string ConstraintQuery = $@"
                                            SELECT acc.column_name, ac.constraint_type
                                            FROM all_cons_columns acc
                                            JOIN all_constraints ac 
                                            ON ac.constraint_name = acc.constraint_name AND ac.owner = acc.owner
                                            WHERE acc.owner = :owner 
                                            AND acc.table_name = :tableName 
                                            AND ac.constraint_type = :constraintType";



                // Execute the queries in parallel
                Task<List<ConstraintInfo>> sourceConstraintTask = Task.Run(() => ExecuteQueryToList(sourceConnection, ConstraintQuery, owner, tableName, constraint));
                Task<List<ConstraintInfo>> targetConstraintTask = Task.Run(() => ExecuteQueryToList(targetConnection, ConstraintQuery, owner, tableName, constraint));

                Task.WaitAll(sourceConstraintTask, targetConstraintTask);

                var sourceConstraintColumns = sourceConstraintTask.Result.Select(ci => $"{ci.ColumnName} with constraint {ci.ConstraintType} {GetConstraintTypeName(ci.ConstraintType)}").ToList();
                var targetConstraintColumns = targetConstraintTask.Result.Select(ci => $"{ci.ColumnName} with constraint {ci.ConstraintType} {GetConstraintTypeName(ci.ConstraintType)}").ToList();


                // Find constraints that are in source but not in target
                var columnsWithConstraintsMissingInTarget = sourceConstraintColumns.Except(targetConstraintColumns).ToList();
                foreach (var missingColumn in columnsWithConstraintsMissingInTarget)
                {
                    string sourceToTargetMsg = $"Column with constraint '{missingColumn}' is present in {sourceDatabaseName} but missing in {targetDatabaseName} for table {owner}.{tableName}.";
                    outputFile.WriteLine(sourceToTargetMsg);
                    Console.WriteLine(sourceToTargetMsg);
                }

                // Find columns with constraints that are in target but not in source
                var columnsWithConstraintsMissingInSource = targetConstraintColumns.Except(sourceConstraintColumns).ToList();
                foreach (var missingColumn in columnsWithConstraintsMissingInSource)
                {
                    string targetToSourceMsg = $"Column with constraint '{missingColumn}' is present in {targetDatabaseName} but missing in {sourceDatabaseName} for table {owner}.{tableName}.";
                    outputFile.WriteLine(targetToSourceMsg);
                    Console.WriteLine(targetToSourceMsg);
                }
            }
        }



        public static List<ConstraintInfo> ExecuteQueryToList(OracleConnection connection, string query, string owner, string tableName, string constraintType)
        {
            List<ConstraintInfo> results = new List<ConstraintInfo>();
            using (OracleCommand command = new OracleCommand(query, connection))
            {
                command.BindByName = true;
                command.Parameters.Add(new OracleParameter("owner", owner));
                command.Parameters.Add(new OracleParameter("tableName", tableName));
                command.Parameters.Add(new OracleParameter("constraintType", constraintType));

                using (OracleDataReader reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        string columnName = reader.GetString(0);
                        string constrType = reader.GetString(1);
                        results.Add(new ConstraintInfo(columnName, constrType));
                    }
                }
            }
            return results;
        }


        public static bool CompareLists(List<string> list1, List<string> list2)
        {
            HashSet<string> set1 = new HashSet<string>(list1);
            HashSet<string> set2 = new HashSet<string>(list2);

            // Check if the sets of elements are equal
            return set1.SetEquals(set2);
        }

        public static string GetConstraintTypeName(string constraintType)
        {
            switch (constraintType)
            {
                case "P":
                    return "Primary Key";
                case "U":
                    return "Unique Constraint";
                case "R":
                    return "Foreign Key";
                case "C":
                    return "Check Constraint";
                default:
                    return "Unknown Constraint Type";
            }
        }
        public static void RemoveDuplicateRowsAndSortFile(string filePath)
        {
            try
            {
                // Read all lines from the file
                var lines = File.ReadAllLines(filePath);

                // Remove duplicates and sort based on the first word
                var sortedUniqueLines = lines.Distinct()
                                             .OrderBy(line => line.Split(' ').FirstOrDefault())
                                             .ToList();

                // Write the sorted, unique lines back to the file
                File.WriteAllLines(filePath, sortedUniqueLines);

                Console.WriteLine("Duplicate rows removed and file sorted successfully.");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"An error occurred: {ex.Message}");
            }
        }
        public class ConstraintInfo
        {
            public string ColumnName { get; set; }
            public string ConstraintType { get; set; }

            public ConstraintInfo(string columnName, string constraintType)
            {
                ColumnName = columnName;
                ConstraintType = constraintType;
            }
        }


    }
}

