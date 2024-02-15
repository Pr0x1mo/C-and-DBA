using Oracle.DataAccess.Client;
using Oracle.DataAccess.Types;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Management;
using System.Text;
using System.Threading.Tasks;

namespace OracleDBATasks
{
    internal class Weekly
    {
       public static void Weekly_Tasks()
        {
            // Call CheckListenerServices

            foreach (string dbName in Daily.connectionStrings.Keys)
            {
                string connectionString = Daily.connectionStrings[dbName];

                using (OracleConnection connection = new OracleConnection(connectionString))
                {
                    //try
                    //{
                        connection.Open();

                    // Task 1: Check the objects fragmented
                    List<string> fragmentedObjectsResult = CheckFragmentedObjects(connection);
                    PrintResults(dbName, "Fragmented Objects", fragmentedObjectsResult);

                    Console.ReadLine();
                    // Task 2: Check Chaining & Migrated Rows
                    List<string> chainingAndMigratedRowsResult = CheckChainingAndMigratedRows(connection);
                    PrintResults(dbName, "Chaining & Migrated Rows", chainingAndMigratedRowsResult);
                    Console.ReadLine();
                    // Task 3: Check the size of tables & check whether they need to partition or not
                    string tableSizeQuery = "SELECT OWNER, TABLE_NAME, BLOCKS FROM DBA_TABLES WHERE NUM_ROWS > 0";
                    List<string> tableSizeResult = CheckTableSizeAndPartition(connection, tableSizeQuery);
                    PrintResults(dbName, "Table Size & Partition Check", tableSizeResult);
                    Console.ReadLine();
                    // Task 4: Check for block corruption
                    List<string> blockCorruptionResult = CheckBlockCorruption(connection);
                    PrintResults(dbName, "Block Corruption Detected for ", blockCorruptionResult);
                    Console.ReadLine();

                    // Task 5: Check Tables Without Primary Keys
                    List<string> tablesWithoutPKResult = CheckTablesWithoutPK(connection);
                    PrintResults(dbName, "Tables Without Primary Keys", tablesWithoutPKResult);
                    Console.ReadLine();
                    // Task 6: Check tables without an index
                    List<string> tablesWithoutIndexResult = CheckTablesWithoutIndex(connection);
                    PrintResults(dbName, "Tables Without Indexes", tablesWithoutIndexResult);
                    Console.ReadLine();
                    // Task 7: Check tables having more indexes

                    List<string> tablesMoreIndexesResult = CheckTablesWithMoreIndexes(connection);
                    PrintResults(dbName, "Tables Having More Indexes", tablesMoreIndexesResult);
                    Console.ReadLine();
                    // Task 8: Check the tables having FK but there is no Index
                    List<string> tablesFKWithoutIndexResult = CheckTablesWithFKWithoutIndex(connection);
                    PrintResults(dbName, "Tables Having Foreign Keys Without Indexes", tablesFKWithoutIndexResult);
                    Console.ReadLine();
                    // Task 9: Check the objects having more extents

                    List<string> objectsWithMoreExtentsResult = CheckObjectsWithMoreExtents(connection);
                    PrintResults(dbName, "Objects Having More Extents", objectsWithMoreExtentsResult);
                    Console.ReadLine();
                    // Task 10: Check the frequently accessed objects and consider placing them in a separate tablespace and in cache


                    List<string> frequentlyAccessedObjectsResult = CheckFrequentlyAccessedObjects(connection);
                    PrintResults(dbName, "Frequently Accessed Objects", frequentlyAccessedObjectsResult);
                    Console.ReadLine();
                    // Task 11: Check the free space at O/s Level

                    List<string> osFreeSpaceResult = CheckOSFreeSpace(connection);
                    PrintResults(dbName, "Free Space at O/s Level", osFreeSpaceResult);
                    Console.ReadLine();
                    // Task 12: Check the CPU and Memory usage at OS level
                    List<string> osUsageResult = CheckOSUsage();
                    PrintResults(dbName, "CPU and Memory Usage at OS Level", osUsageResult);
                    Console.ReadLine();
                    // Task 13: Check the used & free Block at the object level as well as on tablespaces.
                    List<string> usedFreeBlockResult = CheckUsedFreeBlocks(connection);
                    PrintResults(dbName, "Used & Free Blocks", usedFreeBlockResult);
                    Console.ReadLine();
                    // Task 14: Check objects reaching their Max extents
                    List<string> objectsReachedMaxExtentsResult = CheckObjectsReachedMaxExtents(connection);
                    PrintResults(dbName, "Objects Reaching Max Extents", objectsReachedMaxExtentsResult);
                    Console.ReadLine();
                    // Task 15: Check free space in the tablespace
                    List<string> tablespaceFreeSpaceResult = CheckFreeSpaceInTablespace(connection, dbName);
                        
                    PrintResults(dbName, "Free Space in Tablespace", tablespaceFreeSpaceResult);
                    Console.ReadLine();
                    // Task 16: Check invalid objects of the database
                    List<Tuple<string, string, string>> invalidObjectsResult = CheckInvalidObjects(connection);
                        
                    PrintTupleResults(dbName, "Invalid Objects", invalidObjectsResult);
                    Console.ReadLine();
                    // Recompile invalid objects
                    RecompileInvalidObjects(connection, invalidObjectsResult);
                    Console.ReadLine();
                    // Task 17: Check open cursors not reaching the max limit
                    CheckOpenCursors(connection);
                    Console.ReadLine();
                    // Task 18: Check locks not reaching the max lock
                    CheckLocksNotReachingMaxLock(connection);
                    Console.ReadLine();
                    // Task 19: Check free quota limited available for each user
                    CheckFreeQuotaLimitedAvailable(connection);
                    Console.ReadLine();
                    // Task 20: Check I/O of each data file
                    CheckDataFileIO(connection);
                    Console.ReadLine();
                    Console.WriteLine("Press Enter to proceed to the next database...");
                        
                    Console.ReadLine();
                   // }
                    //catch (Exception ex)
                    //{
                    //    // Handle exceptions here
                    //    Console.WriteLine($"Error for {dbName}: {ex.Message}");
                    //}
                }
            }
        }// end of weekly 



        public static void PrintResults(string dbName, string taskName, List<string> results)
        {
            Console.WriteLine($"Results for {dbName} - {taskName}:");
            Console.WriteLine();

            if (results.Count > 0)
            {
                foreach (string result in results)
                {
                    Console.WriteLine(result);
                }
            }
            else
            {
                Console.WriteLine("No issues found.");
            }

            Console.WriteLine();
        } // printresults 

        public static List<string> CheckFragmentedObjects(OracleConnection connection)
        {
            string query = @"
                            SELECT owner, segment_name, segment_type, HEADER_FILE, HEADER_BLOCK, BYTES, BLOCKS, EXTENTS,
                                INITIAL_EXTENT, NEXT_EXTENT, MIN_EXTENTS, MAX_EXTENTS, MAX_SIZE,
                                ROUND(((NEXT_EXTENT - INITIAL_EXTENT) / INITIAL_EXTENT) * 100, 2) AS PERCENTAGE_GROWTH
                            FROM dba_segments
                            WHERE extents > 10
                            ORDER BY PERCENTAGE_GROWTH DESC
                            FETCH FIRST 20 ROWS ONLY";

            List<string> results = new List<string>();

            using (OracleCommand command = new OracleCommand(query, connection))
            {
                using (OracleDataReader reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        string owner = reader["OWNER"].ToString();
                        string segmentName = reader["SEGMENT_NAME"].ToString();
                        string segmentType = reader["SEGMENT_TYPE"].ToString();
                        // int headerFile = Convert.ToInt32(reader["HEADER_FILE"]);
                        // int headerBlock = Convert.ToInt32(reader["HEADER_BLOCK"]);
                        long bytes = Convert.ToInt64(reader["BYTES"]);
                        int blocks = Convert.ToInt32(reader["BLOCKS"]);
                        int extents = Convert.ToInt32(reader["EXTENTS"]);
                        int initialExtent = Convert.ToInt32(reader["INITIAL_EXTENT"]);
                        int nextExtent = Convert.ToInt32(reader["NEXT_EXTENT"]);
                        // int minExtents = Convert.ToInt32(reader["MIN_EXTENTS"]);
                        // int maxExtents = Convert.ToInt32(reader["MAX_EXTENTS"]);
                        // long maxSize = Convert.ToInt64(reader["MAX_SIZE"]);
                        double percentageGrowth = Convert.ToDouble(reader["PERCENTAGE_GROWTH"]);

                        string resultText = $"Owner: {owner}, Segment Name: {segmentName}, Segment Type: {segmentType}, " +
                            $"Bytes: {bytes}, Blocks: {blocks}, " +
                            $"Extents: {extents}, Initial Extent: {initialExtent}, Next Extent: {nextExtent}, " +
                            $"Percentage Growth: {percentageGrowth}%";

                        results.Add(resultText);
                    }
                }
            }

            return results;
        } // end of checkfragmentedobjects


        public static List<string> CheckChainingAndMigratedRows(OracleConnection connection)
        {
            string query = @"SELECT DISTINCT dt.owner, 
                                   dt.table_name, 
                                   dt.chain_cnt, 
                                   dt.avg_row_len,
                                   ROUND((SELECT AVG(dtc.avg_row_len) FROM dba_tables dtc WHERE dtc.chain_cnt = 0)) AS baseline_avg_row_len,
                                   ROUND(((dt.avg_row_len - (SELECT AVG(dtc.avg_row_len) FROM dba_tables dtc WHERE dtc.chain_cnt = 0)) / (SELECT AVG(dtc.avg_row_len) FROM dba_tables dtc WHERE dtc.chain_cnt = 0)) * 100) AS deviation_percentage
                   FROM dba_tables dt
                   JOIN dba_tab_columns tc ON dt.owner = tc.owner AND dt.table_name = tc.table_name
                   WHERE dt.chain_cnt > 0 
                   ORDER BY dt.owner, dt.table_name";

            List<string> results = new List<string>();

            using (OracleCommand command = new OracleCommand(query, connection))
            {
                using (OracleDataReader reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        string owner = reader["owner"].ToString();
                        string tableName = reader["table_name"].ToString();
                        int chainCount = Convert.ToInt32(reader["chain_cnt"]);
                        int avgRowLen = Convert.ToInt32(reader["avg_row_len"]);
                        int baselineAvgRowLen = Convert.ToInt32(reader["baseline_avg_row_len"]);
                        double deviationPercentage = Convert.ToDouble(reader["deviation_percentage"]);

                        string resultText = $"Owner: {owner}, Table Name: {tableName}, " +
                        $"Chained Rows: {chainCount}, Average Row Length: {avgRowLen}, " +
                        $"Baseline Avg Row Length: {baselineAvgRowLen}, Deviation Percentage: {deviationPercentage}%";

                        results.Add(resultText);
                    }
                }
            }

            return results;
        } // end of checkchainingmigratedrows

        public static List<string> CheckTableSizeAndPartition(OracleConnection connection, string query)
        {
            List<string> results = new List<string>();

            using (OracleCommand command = new OracleCommand(query, connection))
            {
                using (OracleDataReader reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        string owner = reader["OWNER"] != DBNull.Value ? reader["OWNER"].ToString() : string.Empty;
                        string tableName = reader["TABLE_NAME"] != DBNull.Value ? reader["TABLE_NAME"].ToString() : string.Empty;
                        int blocks = reader["BLOCKS"] != DBNull.Value ? Convert.ToInt32(reader["BLOCKS"]) : 0;

                        // Determine whether partitioning is needed based on some condition
                        bool partitioningNeeded = CheckPartitioningCondition(blocks);
                        if (partitioningNeeded)
                        {
                            string resultText = $"Owner: {owner}, Table Name: {tableName}, Blocks: {blocks} (Partitioning Needed)";
                            results.Add(resultText);
                        }
                    }
                }
            }

            return results;
        } // CheckTableSizeAndPartition

        public static bool CheckPartitioningCondition(int blocks)
        {
            // Replace this with your actual partitioning condition logic
            // For demonstration purposes, we'll assume partitioning is needed if the number of blocks exceeds 1000.
            return blocks > 1000;
        } // end of CheckPartitioningCondition

        public static List<string> CheckFrequentlyAccessedObjects(OracleConnection connection)
        {
            string query = "WITH ExtentInfo AS (" +
                                              "SELECT OWNER, SEGMENT_NAME, SEGMENT_TYPE, TABLESPACE_NAME, COUNT(*) AS EXTENTS, AVG(BLOCKS) AS AVERAGE_BLOCKS " +
                                              "FROM DBA_EXTENTS " +
                                              "GROUP BY OWNER, SEGMENT_NAME, SEGMENT_TYPE, TABLESPACE_NAME " +
                                              "HAVING COUNT(*) > 10" +
                                              ")" +
                                              "SELECT * " +
                                              "FROM (" +
                                              "SELECT e.OWNER, e.SEGMENT_NAME, e.SEGMENT_TYPE, e.TABLESPACE_NAME, e.EXTENTS, Round((e.EXTENTS / (COUNT(*) * e.AVERAGE_BLOCKS))) AS Extent_Density_Index " +
                                              "FROM ExtentInfo e " +
                                              "JOIN dba_segments s ON e.OWNER = s.OWNER AND e.SEGMENT_NAME = s.SEGMENT_NAME " +
                                              "AND e.SEGMENT_TYPE = s.SEGMENT_TYPE AND e.TABLESPACE_NAME = s.TABLESPACE_NAME " +
                                              "GROUP BY e.OWNER, e.SEGMENT_NAME, e.SEGMENT_TYPE, e.TABLESPACE_NAME, e.EXTENTS, e.AVERAGE_BLOCKS " +
                                              ")" +
                                              "WHERE Extent_Density_Index > 2 " +
                                              "ORDER BY Extent_Density_Index DESC";
            List<string> results = new List<string>();

            try
            {
                using (OracleCommand command = new OracleCommand(query, connection))
                using (OracleDataReader reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        string owner = reader["OWNER"].ToString();
                        string segmentName = reader["SEGMENT_NAME"].ToString();
                        string segmentType = reader["SEGMENT_TYPE"].ToString();
                        string tablespaceName = reader["TABLESPACE_NAME"].ToString();
                        string extents = reader["EXTENTS"].ToString();
                        string extentDensityIdx = reader["Extent_Density_Index"].ToString();

                        string result = $"Owner: {owner}, Segment Name: {segmentName}, Segment Type: {segmentType}, Tablespace: {tablespaceName}, Extents: {extents}, Extent Density Idx: {extentDensityIdx}";
                        results.Add(result);
                    }
                }
            }
            catch (Exception ex)
            {
                results.Add($"Error: {ex.Message}");
            }

            return results;
        } //checkfrequentlyaccessobjects

        public static List<string> CheckBlockCorruption(OracleConnection connection)
        {
            string query = "SELECT OWNER, SEGMENT_NAME, SEGMENT_TYPE FROM DBA_EXTENTS WHERE FILE_ID = 0 AND 65535 = block_id AND segment_type != 'TEMPORARY'";

            List<string> results = new List<string>();
            using (OracleCommand command = new OracleCommand(query, connection))
            {
                using (OracleDataReader reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        results.Add($"{reader["OWNER"]}.{reader["SEGMENT_NAME"]} (Type: {reader["SEGMENT_TYPE"]})");
                    }
                }
            }
            return results;
        } //CheckBlockCorruption


        public static List<string> CheckTablesWithoutPK(OracleConnection connection)
        {
            string query = "SELECT OWNER, TABLE_NAME FROM DBA_TABLES WHERE OWNER NOT IN ('SYS', 'SYSTEM') AND TABLE_NAME NOT IN (SELECT TABLE_NAME FROM DBA_CONSTRAINTS WHERE CONSTRAINT_TYPE = 'P')";

            List<string> results = new List<string>();
            using (OracleCommand command = new OracleCommand(query, connection))
            {
                using (OracleDataReader reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        string owner = reader["OWNER"] != DBNull.Value ? reader["OWNER"].ToString() : string.Empty;
                        string tableName = reader["TABLE_NAME"] != DBNull.Value ? reader["TABLE_NAME"].ToString() : string.Empty;

                        string resultText = $"Owner: {owner}, Table Name: {tableName} (No Primary Key)";
                        results.Add(resultText);
                    }
                }
            }
            return results;
        } // end of checktableswithoutpk

        public static List<string> CheckTablesWithoutIndex(OracleConnection connection)
        {
            string query  = "SELECT OWNER, TABLE_NAME FROM DBA_TABLES WHERE OWNER NOT IN ('SYS', 'SYSTEM') AND TABLE_NAME NOT IN (SELECT TABLE_NAME FROM DBA_INDEXES)";

            List<string> results = new List<string>();
            using (OracleCommand command = new OracleCommand(query, connection))
            {
                using (OracleDataReader reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        string owner = reader["OWNER"] != DBNull.Value ? reader["OWNER"].ToString() : string.Empty;
                        string tableName = reader["TABLE_NAME"] != DBNull.Value ? reader["TABLE_NAME"].ToString() : string.Empty;

                        string resultText = $"Owner: {owner}, Table Name: {tableName} (No Indexes)";
                        results.Add(resultText);
                    }
                }
            }
            return results;
        } // CheckTablesWithoutIndex


        public static List<string> CheckTablesWithMoreIndexes(OracleConnection connection)
        {
            string query = "SELECT OWNER, TABLE_NAME, COUNT(*) AS NUM_INDEXES FROM DBA_INDEXES GROUP BY OWNER, TABLE_NAME HAVING COUNT(*) > 5";
            List<string> results = new List<string>();
            using (OracleCommand command = new OracleCommand(query, connection))
            {
                using (OracleDataReader reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        string owner = reader["OWNER"] != DBNull.Value ? reader["OWNER"].ToString() : string.Empty;
                        string tableName = reader["TABLE_NAME"] != DBNull.Value ? reader["TABLE_NAME"].ToString() : string.Empty;
                        int numIndexes = reader["NUM_INDEXES"] != DBNull.Value ? Convert.ToInt32(reader["NUM_INDEXES"]) : 0;

                        string resultText = $"Owner: {owner}, Table Name: {tableName}, Number of Indexes: {numIndexes} (More than 5)";
                        results.Add(resultText);
                    }
                }
            }
            return results;
        } // end of checktablewithmoreindexes


        public static List<string> CheckTablesWithFKWithoutIndex(OracleConnection connection)
        {
            string query = "SELECT DISTINCT tc.OWNER, tc.TABLE_NAME FROM DBA_CONSTRAINTS tc JOIN DBA_CONSTRAINTS rc ON tc.R_CONSTRAINT_NAME = rc.CONSTRAINT_NAME WHERE tc.CONSTRAINT_TYPE = 'R' AND rc.INDEX_OWNER IS NULL";

            List<string> results = new List<string>();
            using (OracleCommand command = new OracleCommand(query, connection))
            {
                using (OracleDataReader reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        string owner = reader["TABLE_OWNER"] != DBNull.Value ? reader["TABLE_OWNER"].ToString() : string.Empty;
                        string tableName = reader["TABLE_NAME"] != DBNull.Value ? reader["TABLE_NAME"].ToString() : string.Empty;

                        string resultText = $"Owner: {owner}, Table Name: {tableName}";
                        results.Add(resultText);
                    }
                }
            }
            return results;
        } //CheckTablesWithFKWithoutIndex


        public static List<string> CheckObjectsWithMoreExtents(OracleConnection connection )
        {
            string query = "SELECT OWNER, SEGMENT_NAME, SEGMENT_TYPE, EXTENTS FROM DBA_SEGMENTS WHERE EXTENTS > 10 ORDER BY EXTENTS DESC";
            List<string> results = new List<string>();

            using (OracleCommand command = new OracleCommand(query, connection))
            {
                using (OracleDataReader reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        string owner = reader["OWNER"] != DBNull.Value ? reader["OWNER"].ToString() : string.Empty;
                        string segmentName = reader["SEGMENT_NAME"] != DBNull.Value ? reader["SEGMENT_NAME"].ToString() : string.Empty;
                        string segmentType = reader["SEGMENT_TYPE"] != DBNull.Value ? reader["SEGMENT_TYPE"].ToString() : string.Empty;
                        int extents = reader["EXTENTS"] != DBNull.Value ? Convert.ToInt32(reader["EXTENTS"]) : 0;

                        string resultText = $"Owner: {owner}, Segment Name: {segmentName}, Segment Type: {segmentType}, Extents: {extents} (More than 10)";
                        results.Add(resultText);
                    }
                }
            }

            return results;
        }//CheckObjectsWithMoreExtents

        public static List<string> CheckOSFreeSpace(OracleConnection connection)
        {
            string query = "SELECT TABLESPACE_NAME, FILE_ID,  BLOCK_ID, BYTES / (1024 * 1024) AS FREE_MB, BLOCKS, RELATIVE_FNO FROM DBA_FREE_SPACE ORDER BY TABLESPACE_NAME";
            List<string> results = new List<string>();

            using (OracleCommand command = new OracleCommand(query, connection))
            {
                try
                {
                    using (OracleDataReader reader = command.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            string tablespaceName = reader["TABLESPACE_NAME"].ToString();
                            int fileId = Convert.ToInt32(reader["FILE_ID"]);
                            int blockId = Convert.ToInt32(reader["BLOCK_ID"]);
                            long freeBytes = Convert.ToInt64(reader["FREE_MB"]);
                            long freeBlocks = Convert.ToInt64(reader["BLOCKS"]);
                            int relativeFileNumber = Convert.ToInt32(reader["RELATIVE_FNO"]);

                            string resultLine = $"Tablespace: {tablespaceName}, File ID: {fileId}, Block ID: {blockId}, Free Bytes: {freeBytes}, Free Blocks: {freeBlocks}, Relative File Number: {relativeFileNumber}";
                            results.Add(resultLine);
                        }
                    }
                }
                catch (Exception ex)
                {
                    // Handle exceptions here
                    results.Add($"Error: {ex.Message}");
                }
            }

            return results;
        }//  CheckOSfreespace

        private static  List<string> CheckOSUsage()
        {
            List<string> osUsageResult = new List<string>();

            try
            {
                ManagementObjectSearcher searcher = new ManagementObjectSearcher("root\\CIMV2", "SELECT * FROM Win32_PerfFormattedData_PerfOS_Processor");
                foreach (ManagementObject queryObj in searcher.Get())
                {
                    double cpuUsage = Convert.ToDouble(queryObj["PercentProcessorTime"]);
                    string cpuName = Convert.ToString(queryObj["Name"]);

                    osUsageResult.Add($"CPU Usage ({cpuName}): {cpuUsage}%");
                }

                searcher = new ManagementObjectSearcher("root\\CIMV2", "SELECT * FROM Win32_OperatingSystem");
                foreach (ManagementObject queryObj in searcher.Get())
                {
                    double totalMemory = Convert.ToDouble(queryObj["TotalVisibleMemorySize"]);
                    double freeMemory = Convert.ToDouble(queryObj["FreePhysicalMemory"]);

                    double usedMemoryPercent = 100 - (freeMemory / totalMemory) * 100;

                    osUsageResult.Add($"Used Memory: {usedMemoryPercent}%");
                }
            }
            catch (ManagementException ex)
            {

                Console.WriteLine(ex.Message);
                // Handle exceptions
            }

            return osUsageResult;
        } // end of CheckOSusage

        public static List<string> CheckUsedFreeBlocks(OracleConnection connection)
        {
            List<string> result = new List<string>();

            // Query to get used and free blocks at object level
            string objectBlockQuery = "SELECT OWNER, SEGMENT_NAME, SEGMENT_TYPE, BYTES / (1024 * 1024) AS USED_MB, BLOCKS * 8192 / (1024 * 1024) AS FREE_MB" +
                                      " FROM DBA_SEGMENTS" +
                                      " WHERE OWNER NOT IN ('SYS', 'SYSTEM')";

            // Query to get used and free blocks at tablespace level
            string tablespaceBlockQuery = "SELECT TABLESPACE_NAME, SUM(BYTES) / (1024 * 1024) AS USED_MB, SUM(BLOCKS) * 8192 / (1024 * 1024) AS FREE_MB" +
                                          " FROM DBA_DATA_FILES" +
                                          " GROUP BY TABLESPACE_NAME";

            using (OracleCommand command = new OracleCommand())
            {
                command.Connection = connection;

                // Retrieve and process object level block information
                command.CommandText = objectBlockQuery;
                using (OracleDataReader objectReader = command.ExecuteReader())
                {
                    while (objectReader.Read())
                    {
                        string owner = objectReader["OWNER"].ToString();
                        string segmentName = objectReader["SEGMENT_NAME"].ToString();
                        string segmentType = objectReader["SEGMENT_TYPE"].ToString();
                        double usedMB = Convert.ToDouble(objectReader["USED_MB"]);
                        double freeMB = Convert.ToDouble(objectReader["FREE_MB"]);
                        result.Add($"Object: {owner}.{segmentName} ({segmentType}) | Used: {usedMB} MB, Free: {freeMB} MB");
                    }
                }

                // Retrieve and process tablespace level block information
                command.CommandText = tablespaceBlockQuery;
                using (OracleDataReader tablespaceReader = command.ExecuteReader())
                {
                    while (tablespaceReader.Read())
                    {
                        string tablespaceName = tablespaceReader["TABLESPACE_NAME"].ToString();
                        double usedMB = Convert.ToDouble(tablespaceReader["USED_MB"]);
                        double freeMB = Convert.ToDouble(tablespaceReader["FREE_MB"]);
                        result.Add($"Tablespace: {tablespaceName} | Used: {usedMB} MB, Free: {freeMB} MB");
                    }
                }
            }

            return result;
        } // end of CheckUsedFreeBlocks

        public static List<string> CheckObjectsReachedMaxExtents(OracleConnection connection)
        {
            List<string> result = new List<string>();

            // Query to get objects reaching max extents
            string maxExtentsQuery = "SELECT OWNER, SEGMENT_NAME, SEGMENT_TYPE, EXTENTS, MAX_EXTENTS" +
                                     " FROM DBA_SEGMENTS" +
                                     " WHERE OWNER NOT IN ('SYS', 'SYSTEM') AND EXTENTS >= MAX_EXTENTS";

            using (OracleCommand command = new OracleCommand())
            {
                command.Connection = connection;

                // Retrieve and process objects reaching max extents
                command.CommandText = maxExtentsQuery;
                using (OracleDataReader reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        string owner = reader["OWNER"].ToString();
                        string segmentName = reader["SEGMENT_NAME"].ToString();
                        string segmentType = reader["SEGMENT_TYPE"].ToString();
                        int extents = Convert.ToInt32(reader["EXTENTS"]);
                        int maxExtents = Convert.ToInt32(reader["MAX_EXTENTS"]);
                        result.Add($"Object: {owner}.{segmentName} ({segmentType}) | Extents: {extents}, Max Extents: {maxExtents}");
                    }
                }
            }

            return result;
        } // end of CheckObjectsReachedMaxExtents


        public static  List<string> CheckFreeSpaceInTablespace(OracleConnection connection, string dbName)
        {
            List<string> result = new List<string>();

            // Query to get free space in tablespaces
            string tablespaceFreeSpaceQuery = @"SELECT 
                                              tablespace_name, used_space, tablespace_size, Round(used_percent,2) as used_percent, 
                                              (tablespace_size - used_space) AS free_mb
                                              FROM dba_tablespace_usage_metrics";

            using (OracleCommand command = new OracleCommand())
            {
                command.Connection = connection;

                // Retrieve and process tablespace free space information
                command.CommandText = tablespaceFreeSpaceQuery;
                using (OracleDataReader reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        string tablespaceName = reader["tablespace_name"].ToString();
                        double usedPercent = Convert.ToDouble(reader["used_percent"]);
                        double freeMb = Convert.ToDouble(reader["free_mb"]);
                        Console.WriteLine($"{dbName} {tablespaceName} {usedPercent} {freeMb}");
                        if (usedPercent >= 80.57 && freeMb >= 50)
                        {
                            Console.WriteLine($"{dbName}: TableSpace {tablespaceName} meets criteria");
                        }
                    }
                }
            }

            return result;
        } // end of CheckFreeSpaceInTablespace

        public static List<Tuple<string, string, string>> CheckInvalidObjects(OracleConnection connection)
        {
            List<Tuple<string, string, string>> result = new List<Tuple<string, string, string>>();

            // Query to check invalid objects
            string invalidObjectsQuery = "SELECT OBJECT_NAME, OBJECT_TYPE, OWNER " +
                                         "FROM DBA_OBJECTS " +
                                         "WHERE STATUS = 'INVALID' AND OBJECT_TYPE != 'SYNONYM'";

            using (OracleCommand command = new OracleCommand())
            {
                command.Connection = connection;

                // Retrieve and process invalid object information
                command.CommandText = invalidObjectsQuery;
                using (OracleDataReader invalidObjectReader = command.ExecuteReader())
                {
                    while (invalidObjectReader.Read())
                    {
                        string objectName = invalidObjectReader["OBJECT_NAME"].ToString();
                        string objectType = invalidObjectReader["OBJECT_TYPE"].ToString();
                        string objectOwner = invalidObjectReader["OWNER"].ToString();
                        result.Add(Tuple.Create(objectName, objectType, objectOwner));
                    }
                }
            }
            if (result.Count == 0)
            {
                Console.WriteLine("NO ISSUES FOUND");
            }
            return result;
        }// end of CheckInvalidObjects

        public static void PrintTupleResults(string dbName, string title, List<Tuple<string, string, string>> results)
        {
            Console.WriteLine($"Results for Database: {dbName}");
            Console.WriteLine($"--- {title} ---");

            foreach (var result in results)
            {
                string objectName = result.Item1;
                string objectType = result.Item2;
                string objectOwner = result.Item3;

                Console.WriteLine($"{objectType} {objectOwner}.{objectName}");
            }
        }// end of printTupleResults

        public static void RecompileInvalidObjects(OracleConnection connection, List<Tuple<string, string, string>> invalidObjects)
        {
            // Iterate through the list of invalid objects and generate ALTER statements
            foreach (var invalidObject in invalidObjects)
            {
                string objectName = invalidObject.Item1;
                string objectType = invalidObject.Item2;
                string objectOwner = invalidObject.Item3;

                // Add the schema owner to the ALTER statement
                string alterStatement = $"ALTER {(objectType == "PACKAGE BODY" ? "PACKAGE" : objectType)} {objectOwner}.{objectName} COMPILE";


                Console.WriteLine();
                Console.WriteLine($"cmd: {alterStatement}");

                try
                {
                    // Execute the ALTER statement
                    using (OracleCommand command = new OracleCommand(alterStatement, connection))
                    {
                        // Execute the statement
                        command.ExecuteNonQuery();

                        // Check if there are any warning messages in DBMS_OUTPUT
                        string warningMessage = GetDBMSOutputMessage(connection);
                        if (!string.IsNullOrEmpty(warningMessage))
                        {
                            // Display the warning message
                            Console.WriteLine($"Warning: execution completed with warning - {warningMessage}");
                        }
                        else
                        {
                            // If execution is successful without warnings, display a success message
                            Console.WriteLine($"Compilation successful for {objectOwner}.{objectName}");
                        }
                    }
                }
                catch (OracleException ex)
                {
                    // Handle exceptions here
                    Console.WriteLine($"Error for {objectOwner}.{objectName}: {ex.Message}");
                }
            }
        } // end of RecompileInvalidObjects

        public static string GetDBMSOutputMessage(OracleConnection connection)
        {
            // Capture and return the DBMS_OUTPUT message
            using (OracleCommand command = new OracleCommand("BEGIN DBMS_OUTPUT.GET_LINE(:output, :status); END;", connection))
            {
                command.Parameters.Add(new OracleParameter("output", OracleDbType.Varchar2, ParameterDirection.Output));
                command.Parameters.Add(new OracleParameter("status", OracleDbType.Int32, ParameterDirection.Output));

                command.ExecuteNonQuery();

                string outputMessage = command.Parameters["output"].Value.ToString();
                OracleDecimal status = (OracleDecimal)command.Parameters["status"].Value;

                // Check if the status is not null
                if (!status.IsNull)
                {
                    int statusInt = status.ToInt32();
                    if (statusInt == 0 && !string.IsNullOrEmpty(outputMessage))
                    {
                        return outputMessage;
                    }
                }

                return "";
            }
        }  // GetDBMSOutputMessage

       static void CheckOpenCursors(OracleConnection connection)
        {
            try
            {
                using (OracleCommand command = new OracleCommand("SELECT MAX_OPEN_CURSORS, OPEN_CURSORS FROM V$SESSION WHERE AUDSID = USERENV('SESSIONID')", connection))
                {
                    using (OracleDataReader reader = command.ExecuteReader())
                    {
                        if (reader.Read())
                        {
                            int maxOpenCursors = Convert.ToInt32(reader["MAX_OPEN_CURSORS"]);
                            int openCursors = Convert.ToInt32(reader["OPEN_CURSORS"]);

                            if (openCursors >= maxOpenCursors)
                            {
                                Console.WriteLine($"Warning: Open cursors ({openCursors}) reaching or exceeding the max limit ({maxOpenCursors}).");
                            }
                            else
                            {
                                Console.WriteLine($"Open cursors ({openCursors}) are within acceptable limits (max: {maxOpenCursors}).");
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error checking open cursors: {ex.Message}");
            }
        } // end of CheckOpenCursors

        static void CheckLocksNotReachingMaxLock(OracleConnection connection)
        {
            try
            {
                using (OracleCommand command = new OracleCommand("SELECT MAX_LOCKS, LOCKS FROM V$SESSION WHERE AUDSID = USERENV('SESSIONID')", connection))
                {
                    using (OracleDataReader reader = command.ExecuteReader())
                    {
                        if (reader.Read())
                        {
                            int maxLocks = Convert.ToInt32(reader["MAX_LOCKS"]);
                            int locks = Convert.ToInt32(reader["LOCKS"]);

                            if (locks >= maxLocks)
                            {
                                Console.WriteLine($"Warning: Locks ({locks}) reaching or exceeding the max limit ({maxLocks}).");
                            }
                            else
                            {
                                Console.WriteLine($"Locks ({locks}) are within acceptable limits (max: {maxLocks}).");
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error checking locks: {ex.Message}");
            }
        }// CheckLocksNotReachingMaxLock

        static void CheckFreeQuotaLimitedAvailable(OracleConnection connection)
        {
            try
            {
                // SQL query to check free quota limited available for each user
                string query = "SELECT TABLESPACE_NAME, USERNAME, " +
                               "BYTES / (1024 * 1024) AS USED_MB, " +
                               "MAX_BYTES / (1024 * 1024) AS MAX_BYTES_MB, " +
                               "BLOCKS / (1024 * 1024) AS USED_BLOCKS_MB, " +
                               "MAX_BLOCKS / (1024 * 1024) AS MAX_BLOCKS_MB " +
                               "FROM DBA_TS_QUOTAS";

                using (OracleCommand command = new OracleCommand(query, connection))
                {
                    OracleDataReader reader = command.ExecuteReader();

                    Console.WriteLine("Results for Free Quota Limited Available:");
                    Console.WriteLine("{0,-20} {1,-20} {2,-10} {3,-10} {4,-10} {5,-10}", "TABLESPACE_NAME", "USERNAME", "USED_MB", "MAX_BYTES_MB", "USED_BLOCKS_MB", "MAX_BLOCKS_MB");
                    while (reader.Read())
                    {
                        string tablespaceName = reader["TABLESPACE_NAME"].ToString();
                        string username = reader["USERNAME"].ToString();
                        double usedMB = Convert.ToDouble(reader["USED_MB"]);
                        double maxBytesMB = Convert.ToDouble(reader["MAX_BYTES_MB"]);
                        double usedBlocksMB = Convert.ToDouble(reader["USED_BLOCKS_MB"]);
                        double maxBlocksMB = Convert.ToDouble(reader["MAX_BLOCKS_MB"]);

                        Console.WriteLine("{0,-20} {1,-20} {2,-10} {3,-10} {4,-10} {5,-10}", tablespaceName, username, usedMB, maxBytesMB, usedBlocksMB, maxBlocksMB);
                    }

                    reader.Close();
                }

            }
            catch (Exception ex)
            {
                // Handle exceptions here
                Console.WriteLine("Error: " + ex.Message);
            }
        }// CheckFreeQuotaLimitedAvailable

        static void CheckDataFileIO(OracleConnection connection)
        {
            try
            {
                // SQL query to check I/O of each data file
                string query = "SELECT v$datafile.name AS File_Name, v$filestat.phyrds AS Reads, v$filestat.phywrts AS Writes " +
                               "FROM v$filestat, v$datafile " +
                               "WHERE v$filestat.file# = v$datafile.file#";

                using (OracleCommand command = new OracleCommand(query, connection))
                {
                    OracleDataReader reader = command.ExecuteReader();

                    Console.WriteLine("Data File I/O:");
                    Console.WriteLine("{0,-50} {1,-20} {2,-20}", "File_Name", "Reads", "Writes");
                    while (reader.Read())
                    {
                        string fileName = reader["File_Name"].ToString();
                        long reads = Convert.ToInt64(reader["Reads"]);
                        long writes = Convert.ToInt64(reader["Writes"]);

                        Console.WriteLine("{0,-50} {1,-20} {2,-20}", fileName, reads, writes);
                    }

                    reader.Close();
                }
            }
            catch (Exception ex)
            {
                // Handle exceptions here
                Console.WriteLine("Error: " + ex.Message);
            }
        } // end of checkdatafileIO


    } //end of weekly 

} // end of weelkly class
