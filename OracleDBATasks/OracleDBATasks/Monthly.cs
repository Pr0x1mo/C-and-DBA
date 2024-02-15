using Oracle.DataAccess.Client;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace OracleDBATasks
{

    internal class Monthly
    {

        public static void Monthly_Tasks()
        {
            //Task 1: Note: Check the “alert log” file to verify if there is dead lock occur while running the application,
            //If any dead lock found then follow the Oracle doc “Dealock” to avoid the same in future.

            CheckForDeadlocks();

            //// Task 2: Check the database size & compare it to the previous size to find the exact growth of the database
            CheckDatabaseSize();

            //// task 3 Find tablespace Status, segment management, initial & Max Extents etc from dba_tablespaces
            findTableSpaceStatus();
            //// task 4 Check location of datafile & Used & free space of each datafile
            CheckDatafileSpace();
            //// task 5 Check default tablespace & temporary tablespace of each user
            CheckUserTablespaces();
            //// task 6 Check the Indexes which is not used yet
            CheckUnusedIndexes();
            //// task 7 Check the stat pack report to find the overall database performance & follow the database performance document to make sure database well optimized.
            //CheckStatspackReport();
            ////task 8 Tablespace need coalescing
            CheckTablespacesForCoalescing();
            //// task 9 trend analaysis of tablespace
            TablespaceTrendAnalysis();

        }




        static void TablespaceTrendAnalysis()
        {
            try
            {
                foreach (var dbName in Daily.connectionStrings.Keys)
                {
                    var connectionString = Daily.connectionStrings[dbName];

                    using (var connection = new OracleConnection(connectionString))
                    {
                        connection.Open();

                        string trendAnalysisQuery = @"
                    SELECT ts.name, 
                    ROUND((SUM(bytes)/1024/1024)/MAX(CEIL(MONTHS_BETWEEN(SYSDATE,creation_time))),0) AS ""AVG_SIZE_MB"", 
                    ROUND((SUM(bytes)/1024/1024)/MAX(CEIL(MONTHS_BETWEEN(SYSDATE,creation_time)))* 3,0) AS ""SPACE_NEXT_3_MONTHS_MB"" 
                    FROM sys.v_$datafile dt, v$tablespace ts 
                    WHERE dt.ts# = ts.ts# 
                    GROUP BY ts.name";

                        using (OracleCommand command = new OracleCommand(trendAnalysisQuery, connection))
                        {
                            using (OracleDataReader reader = command.ExecuteReader())
                            {
                                Console.WriteLine($"Trend Analysis for {dbName}:");
                                Console.WriteLine($"{"Tablespace",-30} | {"Avg. Monthly Growth (MB)",-25} | {"Est. Space Next 3 Months (MB)",-30}");
                                Console.WriteLine(new string('-', 90));  // This will print a separator line

                                while (reader.Read())
                                {
                                    string tablespaceName = reader["name"].ToString();
                                    string avgSizeMb = reader["AVG_SIZE_MB"].ToString();
                                    string spaceNext3MonthsMb = reader["SPACE_NEXT_3_MONTHS_MB"].ToString();

                                    Console.WriteLine($"{tablespaceName,-30} | {avgSizeMb,25} | {spaceNext3MonthsMb,30}");
                                }
                            }
                        }
                    }
                    Console.WriteLine("Press Enter for Next Database");
                    Console.ReadLine();

                }
            }
            catch (OracleException ex)
            {
                Console.WriteLine("Error in Tablespace Trend Analysis: " + ex.Message);
            }
        }



        static void CheckTablespacesForCoalescing()
        {
            try
            {
                foreach (var dbName in Daily.connectionStrings.Keys)
                {
                    var connectionString = Daily.connectionStrings[dbName];

                    using (var connection = new OracleConnection(connectionString))
                    {
                        connection.Open();

                        string query = @"SELECT tablespace_name, percent_extents_coalesced 
                                 FROM dba_free_space_coalesced 
                                 WHERE percent_extents_coalesced <> 100";

                        using (OracleCommand command = new OracleCommand(query, connection))
                        {
                            using (OracleDataReader reader = command.ExecuteReader())
                            {
                                bool hasResults = false;
                                Console.WriteLine($"Tablespaces needing coalescing in {dbName}:");
                                while (reader.Read())
                                {
                                    hasResults = true;
                                    string tablespaceName = reader["tablespace_name"].ToString();
                                    string percentCoalesced = reader["percent_extents_coalesced"].ToString();
                                    Console.WriteLine($"{dbName} Tablespace: {tablespaceName}, Percent Coalesced: {percentCoalesced}%");
                                }

                                if (!hasResults)
                                {
                                    Console.WriteLine("All tablespaces are 100% coalesced.");
                                }
                            }
                        }
                    }
                }
            }
            catch (OracleException ex)
            {
                Console.WriteLine("Error checking tablespaces for coalescing: " + ex.Message);
            }
        }

        static void CheckStatspackReport()
        {
            try
            {
                foreach (var dbName in Daily.connectionStrings.Keys)
                {
                    var connectionString = Daily.connectionStrings[dbName];

                    using (var connection = new OracleConnection(connectionString))
                    {
                        connection.Open();

                        // Sample SQL to fetch the last two snapshots from STATSPACK repository
                        string query = @"SELECT snap_id, snap_time
                                 FROM stats$snapshot
                                 ORDER BY snap_id DESC
                                 FETCH FIRST 2 ROWS ONLY";

                        List<int> snapIds = new List<int>();
                        try
                        {
                            using (OracleCommand command = new OracleCommand(query, connection))
                            {
                                using (OracleDataReader reader = command.ExecuteReader())
                                {
                                    while (reader.Read())
                                    {
                                        snapIds.Add(Convert.ToInt32(reader["snap_id"]));
                                    }
                                }
                            }
                        }
                        catch (OracleException ex)
                        {
                            // Error code for "table or view does not exist" is ORA-00942
                            if (ex.Number == 942)
                            {
                                Console.WriteLine($"Table or view does not exist in {dbName}. Skipping to the next database.");
                                continue;
                            }
                            else
                            {
                                throw; // propagate other errors
                            }
                        }

                        if (snapIds.Count != 2)
                        {
                            Console.WriteLine("Not enough snapshots for a report.");
                            return;
                        }

                        int endSnapId = snapIds[0];
                        int beginSnapId = snapIds[1];

                        // Fetch the difference in key performance metrics between the two snapshots.
                        string diffQuery = $@"SELECT
                                        (end_stats.value - begin_stats.value) AS difference,
                                        end_stats.name
                                      FROM 
                                        stats$sysstat end_stats,
                                        stats$sysstat begin_stats
                                      WHERE 
                                        end_stats.snap_id = {endSnapId} 
                                        AND begin_stats.snap_id = {beginSnapId} 
                                        AND end_stats.name = begin_stats.name 
                                        AND end_stats.name IN ('parse count (total)', 'execute count', 'user commits', 'user rollbacks')";

                        using (OracleCommand diffCommand = new OracleCommand(diffQuery, connection))
                        {
                            using (OracleDataReader reader = diffCommand.ExecuteReader())
                            {
                                Console.WriteLine($"{dbName} Performance metrics difference between snapshots {beginSnapId} and {endSnapId}:");
                                while (reader.Read())
                                {
                                    string metricName = reader["name"].ToString();
                                    int difference = Convert.ToInt32(reader["difference"]);
                                    Console.WriteLine($"{metricName,-30}: {difference}");
                                }
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error checking STATSPACK report: " + ex.Message);
            }
        }// end of checkstatspackreport

        static void CheckForDeadlocks()
        {
            //try
            //{
                foreach (var dbName in Daily.connectionStrings_sys.Keys)
                {
                    var connectionString = Daily.connectionStrings_sys[dbName];

                    using (var connection = new OracleConnection(connectionString))
                    {
                        connection.Open();

                        // Define the query to search for deadlocks in the alert log
                        string query = @"
                                        SELECT record_id AS ""Id"", originating_timestamp AS ""Created on"", message_text AS ""Message Text""
                                        FROM
                                          (SELECT record_id, originating_timestamp, message_text 
                                          FROM sys.x$dbgalertext
                                          WHERE upper(message_text) LIKE '%DEADLOCK DETECTED%'
                                          ORDER BY record_id DESC)
                                        WHERE ROWNUM <= 10
                                        ORDER BY record_id ASC";

                        using (OracleCommand command = new OracleCommand(query, connection))
                        {
                            // Execute the query
                            using (OracleDataReader reader = command.ExecuteReader())
                            {
                                // Check if there are any rows returned
                                if (reader.HasRows)
                                {
                                    Console.WriteLine($"Deadlocks found in {dbName}:");
                                    while (reader.Read())
                                    {
                                        string id = reader["Id"].ToString();
                                        string createdOn = reader["Created on"].ToString();
                                        string messageText = reader["Message Text"].ToString();

                                        // Output the deadlock information
                                        Console.WriteLine($"Id: {id}, Created on: {createdOn}, Message: {messageText}");
                                    }
                                }
                                else
                                {
                                    Console.WriteLine($"No deadlocks found in {dbName}.");
                                }
                            }
                        }
                    }
                }
            //}
            //catch (OracleException ex)
            //{
            //    Console.WriteLine("Error checking for deadlocks: " + ex.Message);
            //}
        }


        static void CheckDatabaseSize()
        {
            try
            {
                // Get the total months (myRange) and maximum increments (maxinc)
                int myRange = 0;
                int maxinc = 0;

                // Execute the SQL queries to fetch myRange and maxinc
                foreach (var dbName in Daily.connectionStrings.Keys)
                {
                    var connectionString = Daily.connectionStrings[dbName];

                    using (var connection = new OracleConnection(connectionString))
                    {
                        connection.Open();

                        // Fetch myRange
                        using (OracleCommand myRangeCommand = new OracleCommand("SELECT CEIL(MONTHS_BETWEEN(SYSDATE, created)) tot_mon FROM v$database", connection))
                        {
                            myRange = Convert.ToInt32(myRangeCommand.ExecuteScalar());
                        }

                        // Fetch maxinc
                        using (OracleCommand maxincCommand = new OracleCommand("SELECT MAX(ROUND((d.bytes - d.create_bytes) / f.inc / d.block_size)) maxinc FROM sys.file$ f, v$datafile d WHERE f.inc > 0 AND f.file# = d.file# AND d.bytes > d.create_bytes", connection))
                        {
                            maxinc = Convert.ToInt32(maxincCommand.ExecuteScalar());
                        }


                        // Use myRange and maxinc in your query
                        string query = $@"with extended_files as
                                                (select 
                                                        file#,
                                                        nvl(lag(file_size, 1) over (partition by file# order by file_size), 0)
                                                        prior_size,
                                                        file_size,
                                                        block_size
                                                 from (select f.file#,
                                                              f.create_blocks + x.rn * f.inc file_size,
                                                              f.block_size     
                                                       from (select f.file#,
                                                                    d.create_bytes / d.block_size create_blocks,
                                                                    f.inc,
                                                                    d.bytes / d.block_size blocks,
                                                                    d.block_size
                                                             from sys.file$ f,
                                                                  v$datafile d    
                                                             where f.inc > 0
                                                               and f.file# = d.file#
                                                               and d.bytes > d.create_bytes
                                                               and rownum > 0) f,
                                                            (select rownum - 1 rn
                                                             from dual
                                                             connect by level <= {maxinc} + 1) x
                                                       where (f.create_blocks + x.rn * f.inc) <= f.blocks))
                                    select ""MONTH"",
                                           round(cumul/1024, 2) GB,
                                           -- Draw a histogram
                                           --rpad('=', round(60 * cumul / current_M), '=') volume
                                           NVL(ROUND((cumul/1024 - LAG(cumul/1024) OVER (ORDER BY mon)) / NULLIF(LAG(cumul/1024) OVER (ORDER BY mon), 0) * 100, 2),0) AS increase_percentage,
                                           NVL(ROUND(cumul/1024 / NULLIF(LAG(cumul/1024) OVER (ORDER BY mon), 0), 2), 0) AS factor_increase
                                    from

                                    (select to_char(cal.mon, 'MON-YYYY') ""MONTH"",
                                                 sum(nvl(evt.M, 0)) over (order by cal.mon range unbounded preceding) cumul,
                                                 tot.curr_M current_M,
                                                 cal.mon
                                          from -- current database size (data size)
                                               (select round(sum(bytes)/1024/1024) curr_M
                                                from v$datafile) tot,
                                               -- all the months since the database was created
                                               (select add_months(trunc(sysdate, 'MONTH'), -rn) mon
                                                from (select rownum - 1 rn
                                                      from dual
                                                      connect by level <= {myRange})) cal,
                                               -- all the months when the size of the database changed
                                               (select size_date,
                                                       round(sum(bytes)/1024/1024) M
                                                from (-- files in autoextend mode
                                                      select file#, max(bytes) bytes, size_date
                                                      from (select file#, bytes, trunc(min(ctime), 'MONTH') size_date
                                                            -- Get the oldest creation date of tables or indexes
                                                            -- that are located in extensions.
                                                            -- Other segment types are ignored.
                                                            from (select s.file#,
                                                                         f.file_size * f.block_size bytes,
                                                                         o.ctime
                                                                  from sys.seg$ s,
                                                                       extended_files f,
                                                                       sys.tab$ t,
                                                                       sys.obj$ o
                                                                  where s.file# = f.file#
                                                                    and s.type# = 5
                                                                    and s.block# between f.prior_size and f.file_size
                                                                    and s.file# = t.file#
                                                                    and s.block# = t.block#
                                                                    and t.obj# = o.obj#
                                                                  union all
                                                                  select s.file#,
                                                                         f.file_size * f.block_size bytes,
                                                                         o.ctime
                                                                  from sys.seg$ s,
                                                                       extended_files f,
                                                                       sys.ind$ i,
                                                                       sys.obj$ o
                                                                  where s.file# = f.file#
                                                                    and s.type# = 6
                                                                    and s.block# between f.prior_size and f.file_size
                                                                    and s.file# = i.file#
                                                                    and s.block# = i.block#
                                                                    and i.obj# = o.obj#)
                                                            group by file#, bytes)
                                                      group by file#, size_date
                                                      union all
                                                      -- files that are not in autoextend mode
                                                      select d.file#,
                                                             d.create_bytes bytes,
                                                             trunc(d.creation_time, 'MONTH') size_date
                                                      from v$datafile d,
                                                           sys.file$ f
                                                      where nvl(f.inc, 0) = 0
                                                        and f.file# = d.file#)
                                                group by size_date) evt
                                          where evt.size_date (+) = cal.mon)
                                    order by mon";

                        using (OracleCommand queryCommand = new OracleCommand(query, connection))
                        {

                            using (OracleDataReader reader = queryCommand.ExecuteReader())
                            {
                                Console.WriteLine($"{dbName,-15}");
                                Console.WriteLine($"{"MONTH",-15} {"GB",-15} {"INCREASE%",-10} {"FACTOR",-10}");
                                double totalGbFirstMonth = 0.0;
                                double totalGbLastMonth = 0.0;
                                while (reader.Read())
                                {
                                    string month = reader["MONTH"].ToString();
                                    double gb = Convert.ToDouble(reader["GB"]);
                                    double increasePercentage = Convert.ToDouble(reader["INCREASE_PERCENTAGE"]);
                                    double factorIncrease = Convert.ToDouble(reader["FACTOR_INCREASE"]);

                                    Console.WriteLine($"{month,-15} {gb,-15} {increasePercentage, -10} {factorIncrease,-10}");

                                    if (totalGbFirstMonth == 0.0)
                                    {
                                        totalGbFirstMonth = gb;
                                    }

                                    totalGbLastMonth = gb;
                                }

                                double totalPercentageIncrease = ((totalGbLastMonth - totalGbFirstMonth) / totalGbFirstMonth) * 100;
                                double totalFactorIncrease = totalGbLastMonth / totalGbFirstMonth;

                                Console.WriteLine($"Total Percentage Increase: {totalPercentageIncrease:F2}%");
                                Console.WriteLine($"Total Factor Increase: {totalFactorIncrease:F2}");
                            } //end of using OracDataReader


                        }// end of using oracleCommand

                    }
                    Console.WriteLine("Press Enter for Next Database");
                    Console.ReadLine();
                } //end of freach DB





            }


            catch (Exception ex)
            {
                Console.WriteLine("Error checking the database size: " + ex.Message);
            }
        } // end of CheckDatabaseSize

        static void findTableSpaceStatus()
        {
            try
            {
                foreach (var dbName in Daily.connectionStrings.Keys)
                {
                    var connectionString = Daily.connectionStrings[dbName];

                    using (var connection = new OracleConnection(connectionString))
                    {
                        connection.Open();

                        string query = @"SELECT tablespace_name, INITIAL_EXTENT, NEXT_EXTENT, MAX_EXTENTS, EXTENT_MANAGEMENT, SEGMENT_SPACE_MANAGEMENT, STATUS
                                FROM dba_tablespaces";

                        using (OracleCommand queryCommand = new OracleCommand(query, connection))
                        {
                            using (OracleDataReader reader = queryCommand.ExecuteReader())
                            {
                                Console.WriteLine($"{dbName}");
                                Console.WriteLine($"{"TABLESPACE_NAME",-20} {"INITIAL_EXTENT",-15} {"NEXT_EXTENT",-15} {"MAX_EXTENTS",-15} {"EXTENT_MANAGEMENT",-20} {"SEGMENT_SPACE_MANAGEMENT",-25} {"STATUS",-10}");
                                while (reader.Read())
                                {
                                    string tablespaceName = reader["TABLESPACE_NAME"].ToString();
                                    string initialExtent = reader["INITIAL_EXTENT"].ToString();
                                    string nextExtent = reader["NEXT_EXTENT"].ToString();
                                    string maxExtents = reader["MAX_EXTENTS"].ToString();
                                    string extentManagement = reader["EXTENT_MANAGEMENT"].ToString();
                                    string segmentSpaceManagement = reader["SEGMENT_SPACE_MANAGEMENT"].ToString();
                                    string status = reader["STATUS"].ToString();

                                    Console.WriteLine($"{tablespaceName,-20} {initialExtent,-15} {nextExtent,-15} {maxExtents,-15} {extentManagement,-20} {segmentSpaceManagement,-25} {status,-10}");
                                }
                            }
                        }
                    }

                    Console.WriteLine("Press Enter for Next Database");
                    Console.ReadLine();
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error finding tablespace status: " + ex.Message);
            }
        }// end of findTableSpaceStatus

        // Task 4: Check location of datafile & Used & free space of each datafile
        static void CheckDatafileSpace()
        {
            try
            {
                foreach (var dbName in Daily.connectionStrings.Keys)
                {
                    var connectionString = Daily.connectionStrings[dbName];

                    using (var connection = new OracleConnection(connectionString))
                    {
                        connection.Open();

                        string query = @"SELECT sysdate, 
                                        SUBSTR(df.NAME, 1, 40) file_name, 
                                        df.bytes / 1024 / 1024 allocated_mb, 
                                        ((df.bytes / 1024 / 1024) - NVL(SUM(dfs.bytes) / 1024 / 1024, 0)) used_mb, 
                                        NVL(SUM(dfs.bytes) / 1024 / 1024, 0) free_space_mb, 
                                        ROUND(((NVL(SUM(dfs.bytes) / 1024 / 1024, 0) / (df.bytes / 1024 / 1024)) * 100), 2) FREE_SPACE_PERC

                                FROM v$datafile df, dba_free_space dfs 
                                WHERE df.file# = dfs.file_id(+)
                                GROUP BY dfs.file_id, df.NAME, df.file#, df.bytes
                                ORDER BY file_name";

                        using (OracleCommand queryCommand = new OracleCommand(query, connection))
                        {
                            using (OracleDataReader reader = queryCommand.ExecuteReader())
                            {
                                Console.WriteLine($"{dbName}");
                                Console.WriteLine($"{"TIMESTAMP",-25} {"FILE_NAME",-45} {"ALLOCATED_MB",-15} {"USED_MB",-15} {"FREE_SPACE_MB",-15} {"FREE_SPACE_PERC",-15}");
                                while (reader.Read())
                                {
                                    string timestamp = reader["SYSDATE"].ToString();
                                    string fileName = reader["FILE_NAME"].ToString();
                                    string allocatedMB = reader["ALLOCATED_MB"].ToString();
                                    string usedMB = reader["USED_MB"].ToString();
                                    string freeSpaceMB = reader["FREE_SPACE_MB"].ToString();
                                    string freespacePercentage = reader["FREE_SPACE_PERC"].ToString();
                                    Console.WriteLine($"{timestamp,-25} {fileName,-45} {allocatedMB,-15} {usedMB,-15} {freeSpaceMB,-15} {freespacePercentage + "%",-15}"); }
                            }
                        }
                    }

                    Console.WriteLine("Press Enter for Next Database");
                    Console.ReadLine();
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error checking datafile space: " + ex.Message);
            }
        }// CheckDatafileSpace

        static void CheckUserTablespaces()
        {
            try
            {
                foreach (var dbName in Daily.connectionStrings.Keys)
                {
                    var connectionString = Daily.connectionStrings[dbName];

                    using (var connection = new OracleConnection(connectionString))
                    {
                        connection.Open();

                        string query = @"SELECT username, default_tablespace FROM dba_users WHERE default_tablespace = 'SYSTEM'";

                        using (OracleCommand queryCommand = new OracleCommand(query, connection))
                        {
                            using (OracleDataReader reader = queryCommand.ExecuteReader())
                            {
                                if (reader.HasRows)
                                {
                                    Console.WriteLine($"{dbName}");
                                    Console.WriteLine($"{"USERNAME",-30} {"DEFAULT TABLESPACE",-30}");
                                    while (reader.Read())
                                    {
                                        string username = reader["USERNAME"].ToString();
                                        string defaultTablespace = reader["DEFAULT_TABLESPACE"].ToString();
                                        Console.WriteLine($"{username,-30} {defaultTablespace,-30}");
                                    }
                                }
                                else
                                {
                                    Console.WriteLine($"{dbName} is good");
                                }
                            }
                        }
                    }

                    Console.WriteLine("Press Enter for Next Database");
                    Console.ReadLine();
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error checking user tablespaces: " + ex.Message);
            }
        } // CheckUserTablespaces

        static void CheckUnusedIndexes()
        {
           
                foreach (var dbName in Daily.connectionStrings.Keys)
            {
                var connectionString = Daily.connectionStrings[dbName];

                using (var connection = new OracleConnection(connectionString))
                {
                    connection.Open();

                    string queryUnusedIndexes = "SELECT *\r\nFROM dba_object_usage\r\nWHERE monitoring = 'YES' and used = 'NO'";


                    using (OracleCommand queryCommand = new OracleCommand(queryUnusedIndexes, connection))
                    {
                        using (OracleDataReader reader = queryCommand.ExecuteReader())
                        {
                            Console.WriteLine($"Unused indexes for:  {dbName}");
                            Console.WriteLine($"{"OWNER",-30} {"INDEX_NAME",-32} {"TABLE_NAME",-30} {"USED",-10}");
                            while (reader.Read())
                            {
                                string owner = reader["OWNER"].ToString();
                                string indexName = reader["INDEX_NAME"].ToString();
                                string tableName = reader["TABLE_NAME"].ToString();
                                string used = reader["USED"].ToString();
                                Console.WriteLine($"{owner, -30} {indexName,-32} {tableName, -30} {used,-10}");
                            }
                        }
                    }

                    Console.WriteLine("Press Enter for Next Database");
                    Console.ReadLine();
                }
            }
 
        }//CheckUnusedIndexes()

    }
}