using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlTypes;
using System.IO;
using System.Linq;
using System.Net;
using System.Reflection;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using System.Xml.Linq;
using Oracle.DataAccess.Client;
using Renci.SshNet;

namespace OracleDBATasks
{
    internal class Daily
    {
        public static Dictionary<string, string> connectionStrings = new Dictionary<string, string>
    {
        { "ASTPRD", "Data Source=(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)(HOST=10.1.1.213)(PORT=1521)))(CONNECT_DATA=(SERVER=DEDICATED)(SERVICE_NAME=astprd_pdb1.prodprisubphx.consortiexpxvcn.oraclevcn.com)));User Id=xborja;Password=Seraphim24$" },
        { "ASTDEV", "Data Source=(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)(HOST=10.0.2.11)(PORT=1521)))(CONNECT_DATA=(SERVER=DEDICATED)(SERVICE_NAME=astdev_pdb1.nonprodprisub.consortiexvcn.oraclevcn.com)));User Id=xborja;Password=Seraphim24$" },
        { "ASTDEMO", "Data Source=(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)(HOST=10.0.2.15)(PORT=1521)))(CONNECT_DATA=(SERVER=DEDICATED)(SERVICE_NAME=astdemo_pdb1.nonprodprisub.consortiexvcn.oraclevcn.com)));User Id=xborja;Password=Seraphim24$" },
        { "ASTSIT", "Data Source=(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)(HOST=10.0.2.14)(PORT=1521)))(CONNECT_DATA=(SERVER=DEDICATED)(SERVICE_NAME=astsit_pdb1.nonprodprisub.consortiexvcn.oraclevcn.com)));User Id=xborja;Password=Seraphim24$" }//,
        //{ "CTXRPT", "Data Source=(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=10.1.1.82)(PORT=1521))(CONNECT_DATA=(SERVER=DEDICATED)(SERVICE_NAME=ctxrpt_pdb1.prodprisubphx.consortiexpxvcn.oraclevcn.com)));User Id=system;Password=ctXU4ea4u" }
    };

        public static Dictionary<string, string> connectionStrings_sys = new Dictionary<string, string>
    {
        { "ASTPRD", "Data Source=(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)(HOST=10.1.1.213)(PORT=1521)))(CONNECT_DATA=(SERVER=DEDICATED)(SERVICE_NAME=astprd_pdb1.prodprisubphx.consortiexpxvcn.oraclevcn.com)));User Id=system;Password=ctXU4ea4u;DBA Privilege=SYSDBA;" },
        { "ASTDEV", "Data Source=(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)(HOST=10.0.2.11)(PORT=1521)))(CONNECT_DATA=(SERVER=DEDICATED)(SERVICE_NAME=astdev_pdb1.nonprodprisub.consortiexvcn.oraclevcn.com)));User Id=system;Password=ctXU4ea4u;DBA Privilege=SYSDBA;" },
        { "ASTDEMO", "Data Source=(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)(HOST=10.0.2.15)(PORT=1521)))(CONNECT_DATA=(SERVER=DEDICATED)(SERVICE_NAME=astdemo_pdb1.nonprodprisub.consortiexvcn.oraclevcn.com)));User Id=system;Password=ctXU4ea4u;DBA Privilege=SYSDBA;" },
        { "ASTSIT", "Data Source=(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)(HOST=10.0.2.14)(PORT=1521)))(CONNECT_DATA=(SERVER=DEDICATED)(SERVICE_NAME=astsit_pdb1.nonprodprisub.consortiexvcn.oraclevcn.com)));User Id=system;Password=ctXU4ea4u;DBA Privilege=SYSDBA;" }
     };
        static void Main(string[] args)
        {
            //Daily_tasks();
            //Weekly.Weekly_Tasks();
            //KillSession.TerminateMySessions();
            //Monthly.Monthly_Tasks();
            TableSpaceIncreaser.TableSpaceIncrease();
            //Nightly.Nightly_Tasks();
            //Faas_Daily_Chks();
            //Faas_Daily_chk.missingIDandNamesMasterPackage();
            //ASH_Analytics.ASH_STAT_REPORT();
            //Apache_Logs.Apache_Log_Getter();
            //RxNorm.WednesdayETL();
            //Create_Index.CreateIndex();
            //CompareUserRoles.CompareUsers();

        }// end of main
        public static void Faas_Daily_Chks()
        {
            Type type = typeof(Faas_Daily_chk);
            foreach (var method in type.GetMethods(BindingFlags.Public | BindingFlags.Static))
            {
                if (method.ReturnType == typeof(void) && method.GetParameters().Length == 0)
                {
                    method.Invoke(null, null);
                }
            }
        }
        static void Daily_tasks()
        {
            foreach (var dbName in connectionStrings.Keys)
            {
                var connectionString = connectionStrings[dbName];

                using (var connection = new OracleConnection(connectionString))
                {
                    try
                    {
                        connection.Open();


                        // Task 1: Check if Oracle Database instance is running & Task 2: check if listener is up

                        ExecuteScalar(dbName, connection);



                        // Task 3: Check for session blocking

                        GetBlockingSessions(dbName, connection);


                        // Task 4: Check the alert log for errors //string IPaddress = ExtractIpAddress(connectionString);

                        CheckAlertLogForErrors(dbName, connectionString);


                        // Task 5: Check for running DBMS jobs and their status

                        CheckRunningDBMSJobs(dbName, connection);


                        // Task 6: Check the top session using more Physical I/O

                        TopSessionWphysicalIO(dbName, connection);


                        // Task 7: Check the number of log switches per hour

                        GetLogSwitchesPerHour(dbName, connection);


                        // Task 8: Check how much redo was generated per hour


                        GetRedoGeneratedPerHour(dbName, connection); // Call the method on the instance



                        // Task 10: Detect lock objects
                        DetectLockedObjects(dbName, connection);


                        // Task 12: Check SQL queries consuming a lot of resources


                        CheckHighResourceSqlQueries(dbName, connection);


                        // Task 13: Check the usage of SGA

                        CheckSGAUsage(dbName, connection);


                        // Task 14: Display database sessions using rollback segments
                        GetRollbackSegments(dbName, connection);


                        // Task 15: Check the state of all DB block buffers
                        GetDBBlockBufferInfo(dbName, connection);


                        GetBufferCacheHitRatio(dbName, connection);
                     

                        Console.WriteLine("Press Enter to proceed to the next database...");
                        Console.ReadLine();
                    }
                    catch (Exception ex)
                    {
                        // Handle exceptions here
                        Console.WriteLine($"Error for {dbName}: {ex.Message}");
                    }
                }
            }
        }// end of Daily();

        // Helper method to execute a scalar query
        static void ExecuteScalar(string dbName, OracleConnection connection)
        {
            var query = "SELECT STATUS FROM V$INSTANCE";
            using (var command = new OracleCommand(query, connection))
            {
               // return command.ExecuteScalar()?.ToString();
                Console.WriteLine($"Oracle Database Instance status for {dbName}: {command.ExecuteScalar()?.ToString()}");
            }
            if (connection.State == ConnectionState.Open)
            {
                Console.WriteLine($"Oracle Database Listener is running for {dbName}.");
            }
            else
            {
                Console.WriteLine($"Oracle Database Listener is not running for {dbName}.");
            }
        }// end of ExecuteScalar

        // Helper method to execute a query and return the results
        public static void TopSessionWphysicalIO(string dbName, OracleConnection connection)
        {
            var query = "select * from(SELECT s.sid,s.serial#,s.username, s.program, ss.value AS physical_io FROM v$session s JOIN v$sesstat ss ON s.sid = ss.SID AND ss.STATISTIC# = 40 WHERE s.type = 'USER' AND ss.value IS NOT NULL ORDER BY ss.value DESC) WHERE rownum <= 10";

          //  var results = new List<Dictionary<string, string>>();
            using (var command = new OracleCommand(query, connection))
            using (var reader = command.ExecuteReader())
            {
                while (reader.Read())
                {
                    StringBuilder rowResult = new StringBuilder();
                    var row = new Dictionary<string, string>();
                    for (int i = 0; i < reader.FieldCount; i++)
                    {
                      //  row[reader.GetName(i)] = reader[i].ToString();
                       // Console.WriteLine($"{reader.GetName(i)}: {reader[i]}");
                        rowResult.Append($"{reader.GetName(i)}: {reader[i]}   ");  // Appending each "column name: value" to the string.

                    }
                   // results.Add(row);
                    Console.WriteLine(rowResult.ToString());
                }
            }
           // return results;
        }// end of ExecuteQuery
        public static string ExtractIpAddress(string connectionString)
        {
            var match = Regex.Match(connectionString, @"HOST=(?<ip>\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})");
            if (match.Success)
            {
                return match.Groups["ip"].Value;
            }
            return null; // or throw an exception if you expect an IP address always
        }

        public static void DownloadFile(string remoteFilePath, string localFilePath, string ipAddress, string username, PrivateKeyFile privateKey)
        {
            using (var scp = new ScpClient(ipAddress, username, privateKey))
            {
                scp.Connect();
                scp.Download(remoteFilePath, new FileInfo(localFilePath));
                scp.Disconnect();
            }
        }
     public static void CheckAlertLogForErrors(string dbName, string connectionString)
        {
            Console.WriteLine($"Alert log error for {dbName}");
            var data = new List<(string Type, string Level, string Timestamp, string Text)>();

            using (var cmd = new OracleCommand())
            {
                cmd.Connection = new OracleConnection(connectionString);
                cmd.CommandText = @"SELECT message_type, message_level, ORIGINATING_TIMESTAMP, MESSAGE_TEXT 
                             FROM V$DIAG_ALERT_EXT 
                             WHERE message_level in (1, 2, 3)
                             ORDER BY ORIGINATING_TIMESTAMP DESC, message_type";

                cmd.Connection.Open();
                using (var reader = cmd.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        data.Add((
                            reader["message_type"].ToString(),
                            reader["message_level"].ToString(),
                            reader["ORIGINATING_TIMESTAMP"].ToString(),
                            reader["MESSAGE_TEXT"].ToString()
                        ));
                    }
                }
                cmd.Connection.Close();
            }

            // Determine the max width for each column
            int maxTypeWidth = Math.Max(data.Max(d => d.Type.Length), "Type".Length);
            int maxLevelWidth = Math.Max(data.Max(d => d.Level.Length), "Level".Length);
            int maxTimestampWidth = Math.Max(data.Max(d => d.Timestamp.Length), "Timestamp".Length);
            int maxTextWidth = Math.Max(data.Max(d => d.Text.Length), "Text".Length);

            var result = new List<string>
            {// Headers           
                $"{"Type".PadRight(maxTypeWidth)} | {"Level".PadRight(maxLevelWidth)} | {"Timestamp".PadRight(maxTimestampWidth)} | {"Text".PadRight(maxTextWidth)}"
            };

            // Add formatted data
            result.AddRange(data.Select(d => $"{d.Type.PadRight(maxTypeWidth)} | {d.Level.PadRight(maxLevelWidth)} | {d.Timestamp.PadRight(maxTimestampWidth)} | {d.Text.PadRight(maxTextWidth)}"));

            foreach (var res in result)
            {
                Console.WriteLine(res);
            }
          //  return result;
        }

        // end of checkalertlogforerrors

        public static List<string> ReadOutput(ShellStream shellStream)
        {
            StreamReader reader = new StreamReader(shellStream, Encoding.UTF8);
            string output = reader.ReadToEnd();

            // Remove control codes (escape sequences)
            // output = Regex.Replace(output, @"\x1B\[[0-?]*[ -/]*[@-~]", "");
            output = Regex.Replace(output, "←]0;oracle@[^ ]+ ", "");

            List<string> lines = output.Split('\n').Where(line => !string.IsNullOrWhiteSpace(line)).ToList();
            return lines;
        }
        public static void SendCommand(ShellStream shellStream, string command)
        {
            StreamWriter writer = new StreamWriter(shellStream);
            writer.WriteLine(command);
            writer.Flush();
        }
        public static void WaitForCommandCompletion(ShellStream shellStream)
        {
            System.Threading.Thread.Sleep(1000); // Adjust the delay as needed.
        }


        // Helper method to retrieve blocking sessions
        static void GetBlockingSessions(string dbName, OracleConnection connection)
        {
            var query = "SELECT blocking_session FROM V$session WHERE blocking_session IS NOT NULL";
            var blockingSessions = new List<string>();
            using (var command = new OracleCommand(query, connection))
            using (var reader = command.ExecuteReader())
            {
                while (reader.Read())
                {
                    blockingSessions.Add(reader[0].ToString());
                }
            }
            if (blockingSessions.Count > 0)
            {
                foreach (var result in blockingSessions)
                {
                    Console.WriteLine($"Blocking session for {dbName}: {result}");
                }
            }
            else
            {
                Console.WriteLine($"No Blocking session for {dbName}");
            }
            //return blockingSessions;
        }// end of getblockingsessions

        public static void CheckRunningDBMSJobs(string dbName, OracleConnection connection)
        {
            var query = "SELECT job_name, state FROM dba_scheduler_jobs WHERE state = 'RUNNING'";
            var runningJobs = new List<string>();

            using (var command = new OracleCommand(query, connection))
            using (var reader = command.ExecuteReader())
            {
                while (reader.Read())
                {
                    runningJobs.Add($"{reader["JOB_NAME"]} ({reader["STATE"]})");
                }
            }

            if (runningJobs.Count > 0)
            {
                Console.WriteLine($"Running DBMS job for {dbName}:");
                foreach (var job in runningJobs)
                {
                    Console.WriteLine(job);
                }
            }
            else
            {
                Console.WriteLine($"No Running DBMS jobs for {dbName}");
            }
        } // end of check running dbmsjobs


        public  static void GetLogSwitchesPerHour(string dbName,OracleConnection connection)
        {
           
            string query = "SELECT TO_CHAR(first_time, 'YYYY-MM-DD HH24'), COUNT(*) FROM v$log_history WHERE first_time >= SYSDATE - 1 GROUP BY TO_CHAR(first_time, 'YYYY-MM-DD HH24') HAVING COUNT(*) > 1 ORDER BY 1";

            List<string> results = new List<string>();
            List<string> logSwitchesResult = new List<string>();    
            using (OracleCommand command = new OracleCommand(query, connection))
            {
                using (OracleDataReader reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        logSwitchesResult.Add($"{reader[0]}: {reader[1]} log switches");
                    }
                }
            }
            if (logSwitchesResult.Count > 0)
            {
                foreach (string result in logSwitchesResult)
                {
                    Console.WriteLine($"Log switches per hour for {dbName}: {result}");
                }
            }
            else
            {
                Console.WriteLine($"Log switches per hour for {dbName}: Everything is normal");
            }
            //return results;
        } // end of getlogswitchesperhour

       public static void GetRedoGeneratedPerHour(string dbName, OracleConnection connection)
        {

            string query = "SELECT TO_CHAR(first_time, 'YYYY-MM-DD HH24'), (MAX(first_change#) - MIN(first_change#)) AS redo_generated FROM v$log_history WHERE first_time >= SYSDATE - 1 GROUP BY TO_CHAR(first_time, 'YYYY-MM-DD HH24') ORDER BY 1";
            // Create an instance of your class
            

            List<string> redoGeneratedResult = new List<string>();
    
            List<string> results = new List<string>();
            using (OracleCommand command = new OracleCommand(query, connection))
            {
                using (OracleDataReader reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        redoGeneratedResult.Add($"{reader[0]}: {reader[1]} bytes of redo generated");
                    }
                }
            }

            if (redoGeneratedResult.Count > 0)
            {
                foreach (string result in redoGeneratedResult)
                {
                    if (!result.Contains("0 bytes of redo generated"))
                    {
                        Console.WriteLine($"Redo generated per hour for {dbName}: {result}");
                    }
                }
            }
            else
            {
                Console.WriteLine($"Redo generated per hour for {dbName}: No redo generated");
            }
          
        } // end of getredogeneratedperhour

        public static void DetectLockedObjects(string dbName, OracleConnection connection)
        {
            string query = "SELECT v.inst_id, d.object_name, v.session_id, v.oracle_username, v.os_user_name, v.process, " +
                                                  "DECODE(v.locked_mode, 0, 'No Lock', 2, 'Row Share', 3, 'Row Exclusive', 4, 'Shared', " +
                                                  "5, 'S/Row Exclusive', 6, 'Exclusive', 'Unknown') Lock_Mode " +
                                                  "FROM gv$locked_object v, dba_objects d " +
                                                  "WHERE v.object_id = d.object_id " +
                                                  "AND d.owner = :OWNER " +
                                                  "AND d.object_name LIKE :OBJECT";

       
           

            List<string> lockedObjectsResult =  new List<string>();
            using (OracleCommand command = new OracleCommand(query, connection))
            {
                command.Parameters.Add(new OracleParameter("OWNER", OracleDbType.Varchar2)).Value = "YourOwnerHere";
                command.Parameters.Add(new OracleParameter("OBJECT", OracleDbType.Varchar2)).Value = "YourObjectNameHere";
                using (OracleDataReader reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        lockedObjectsResult.Add($"Instance ID: {reader["inst_id"]}, Object: {reader["object_name"]}, Session ID: {reader["session_id"]}, " +
                                    $"Oracle Username: {reader["oracle_username"]}, OS User Name: {reader["os_user_name"]}, " +
                                    $"Process: {reader["process"]}, Lock Mode: {reader["Lock_Mode"]}");
                    }
                }
            }
            if (lockedObjectsResult.Count > 0)
            {
                foreach (string result in lockedObjectsResult)
                {
                    Console.WriteLine($"Locked object detected for {dbName}: {result}");
                }
            }
            else
            {
                Console.WriteLine($"Locked objects detected for {dbName}: No locked objects found");
            }
        } // end of detectlockedobjects
        public static void CheckHighResourceSqlQueries(string dbName,  OracleConnection connection)
        {
            string query = "SELECT s.username, sq.sql_id, sq.sql_text, sq.executions, " +
                                         "TO_CHAR(TRUNC(sq.elapsed_time / 3600), 'FM00') || ':' || " +
                                         "TO_CHAR(TRUNC(Mod (sq.elapsed_time, 3600) / 60), 'FM00') || ':' || " +
                                         "TO_CHAR(Mod (sq.elapsed_time, 60), 'FM00') AS elapsed_time_formatted, " +
                                         "TO_CHAR(TRUNC(sq.cpu_time / 3600), 'FM00') || ':' || " +
                                         "TO_CHAR(TRUNC(Mod (sq.cpu_time, 3600) / 60), 'FM00') || ':' || " +
                                         "TO_CHAR(Mod (sq.cpu_time, 60), 'FM00') AS cpu_time_formatted, " +
                                         "sq.buffer_gets, " +
                                         "sq.sql_text " +
                                         "FROM v$sql sq " +
                                         "Join v$session s ON sq.sql_id = s.sql_id " +
                                         "where s.type != 'BACKGROUND' " +
                                         "and s.program not like '%(J0%' " +
                                         "and s.program not like '%rman%' " +
                                         "--and s.last_call_et > (60) " +
                                         "and s.status = 'ACTIVE' " +
                                         "ORDER BY sq.elapsed_time DESC";

            List<string> sqlQueriesResult = new List<string>();
            using (OracleCommand command = new OracleCommand(query, connection))
            {
                using (OracleDataReader reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        sqlQueriesResult.Add($"Username: {reader["USERNAME"]}, SQL ID: {reader["SQL_ID"]}, Elapsed Time: {reader["ELAPSED_TIME_FORMATTED"]}, Sql Text: {reader["sql_text"]}");
                    }
                }
            }
            foreach (string result in sqlQueriesResult)
            {
                Console.WriteLine($"My High resource SQL query for {dbName}: {result}");
                Console.WriteLine();
            }


            Console.WriteLine("moving to Brian's query");


            string query1 = "SELECT s.inst_id , " +
                      "trunc(s.last_call_et / 60,0) MINUTES, " +
                      "s.username DB_USER, " +
                      "s.osuser OS_USER, " +
                      "s.machine MACHINE, " +
                      "s.client_info CLIENT_INFO, " +
                      "s.program PROGRAM, " +
                      "s.status, " +
                      "s.sid SID, " +
                      "s.serial# " +
                      "FROM gv$session s, gv$process p, sys.gv_$sess_io si " +
                      "WHERE s.paddr = p.addr(+) " +
                      "AND si.SID(+) = s.SID " +
                      "and s.TYPE <> 'BACKGROUND' " +
                      "and s.username is not null " +
                      "and s.status = 'ACTIVE' " +
                      "and s.last_call_et > (15 * 60) " +
                      "and s.inst_id=p.inst_id " +
                      "and p.inst_id=si.inst_id " +
                      "and s.program not like '%(J0%' " +
                      "and s.program not like '%rman%' " +
                      "order by 1,2";
            List<string> sqlQueriesResult1 = new List<string>();
            using (OracleCommand command = new OracleCommand(query1, connection))
            {
                using (OracleDataReader reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        sqlQueriesResult1.Add($"DB User: {reader["DB_USER"]}, OS User: {reader["OS_USER"]}, Machine: {reader["MACHINE"]}, Program: {reader["PROGRAM"]}, Status: {reader["status"]}, SID: {reader["SID"]}, Serial: {reader["serial#"]}");
                    }
                }       
            }
            foreach (string result in sqlQueriesResult1)
            {
                Console.WriteLine($"Brian High resource SQL query for {dbName}: {result}");
                Console.WriteLine();
            }
        } // end of checkhighresourcesqlqueries

        public static List<string> CheckHighResourceSqlQueries2(OracleConnection connection, string query)
        {
            List<string> results = new List<string>();
            using (OracleCommand command = new OracleCommand(query, connection))
            {
                using (OracleDataReader reader = command.ExecuteReader())
                {
                    if (reader.HasRows)
                    {
                        while (reader.Read())
                        {
                            results.Add($"Instant ID: {reader["INST_ID"]}, MINUTES: {reader["MINUTES"]}, DB_USER: {reader["DB_USER"]}, OS_USER: {reader["OS_USER"]}, " +
                                        $"MACHINE: {reader["MACHINE"]}, Client info: {reader["CLIENT_INFO"]}, Program: {reader["PROGRAM"]}, STATUS: {reader["STATUS"]}, SID: {reader["SID"]}, SERIAL: {reader["SERIAL#"]}");
                        }
                    }
                    else
                    {
                        results.Add("Brians Query returned nothing");
                    }
                }
            }
            return results;
        } // end of checkhighresourcesqlqueries


        public static void CheckSGAUsage(string dbName, OracleConnection connection)
        {
            string sgaQuery = "SELECT ROUND(SUM(VALUE) / 1024 / 1024, 2) AS sga_size_mb FROM v$sga";
            string sgaSize;
            using (OracleCommand command = new OracleCommand(sgaQuery, connection))
            {
                object result = command.ExecuteScalar();
                sgaSize = result != null ? result.ToString() : "N/A";
            }
           
            Console.WriteLine($"SGA usage for {dbName}: {sgaSize} MB");
        } // end of checksgausage


        public static void GetRollbackSegments(string dbName, OracleConnection connection)
        {

            string query = "SELECT s.sid, s.serial#, s.username, s.program, rs.segment_name FROM v$session s JOIN v$transaction t ON s.saddr = t.ses_addr JOIN dba_rollback_segs rs ON t.xidusn = rs.segment_id";
           
            

            List<string> rollbackSegmentsResult = new List<string>();
            using (OracleCommand command = new OracleCommand(query, connection))
            {
                using (OracleDataReader reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        rollbackSegmentsResult.Add($"Session SID: {reader["SID"]}, Serial#: {reader["SERIAL#"]}, Username: {reader["USERNAME"]}, Program: {reader["PROGRAM"]}, Rollback Segment: {reader["SEGMENT_NAME"]}");
                    }
                }
            }
            if (rollbackSegmentsResult.Count > 0)
            {
                foreach (string result in rollbackSegmentsResult)
                {
                    Console.WriteLine($"Database sessions using rollback segments for {dbName}: {result}");
                }
            }
            else
            {
                Console.WriteLine($"No rollback segments used for {dbName}.");
            }
        } // end of getrollbacksegments

        public static void GetDBBlockBufferInfo(string dbName, OracleConnection connection)
        {
            string query = "select \r\n" +
                "decode(state, 0, 'Free',1, decode(lrba_seq,0,'Available','Being Modified'), 2, 'Not Modified', 3, 'Being Read', 'Other') \"BLOCK STATUS\" ,\r\n" +
                "count(*) cnt \r\n" +
                "from sys.xbh_view  \r\n" +
                "group by decode(state, 0, 'Free', 1, decode(lrba_seq,0,'Available','Being Modified'),2, 'Not Modified', 3, 'Being Read', 'Other')";

            List<string> dbBlockBufferResult = new List<string>();
            using (OracleCommand command = new OracleCommand(query, connection))
            {
                using (OracleDataReader reader = command.ExecuteReader())
                {
                    // Print headers
                    Console.WriteLine($"DB Block Buffer Info for {dbName}: ");
                    Console.WriteLine($"{"BLOCK STATUS",-20} {"CNT",-20}");


                    while (reader.Read())
                    {
                        // Format and add the result
                        dbBlockBufferResult.Add($"{reader["BLOCK STATUS"],-20} {reader["CNT"],-20} ");

                    }
                }
            }

            if (dbBlockBufferResult.Count > 0)
            {
                
                foreach (string result in dbBlockBufferResult)
                {
                    Console.WriteLine(result);
                }
            }
            else
            {
                Console.WriteLine($"No DB Block Buffer information available for {dbName}.");
            }
        }

      

        public static void GetBufferCacheHitRatio(string dbName, OracleConnection connection)
        {

            string query = "SELECT a.value AS \"Block Gets\", b.value AS \"Consistent Gets\", c.value AS \"Physical Reads\", " +
                                  "(ROUND(1 - (c.value / (b.value + a.value)), 5) * 100) AS \"Cache Hit Ratio\" " +
                                  "FROM v$sysstat a, v$sysstat b, v$sysstat c " +
                                  "WHERE a.name = 'db block gets from cache' " +
                                  "AND b.name = 'consistent gets from cache' " +
                                  "AND c.name = 'physical reads cache'";
           
            List<string> bufferCacheHitRatioResult = new List<string>();
            using (OracleCommand command = new OracleCommand(query, connection))
            {
                using (OracleDataReader reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        bufferCacheHitRatioResult.Add($"Block Gets: {reader["Block Gets"]}, Consistent Gets: {reader["Consistent Gets"]}, " +
                                    $"Physical Reads: {reader["Physical Reads"]}, Cache Hit Ratio: {reader["Cache Hit Ratio"]}%");
                    }
                }
            }

            foreach (string result in bufferCacheHitRatioResult)
            {
                Console.WriteLine($"Buffer Cache Hit Ratio for {dbName}: {result}");

                // Extract the Cache Hit Ratio value from the result string and convert it to a double.
                double cacheHitRatio = Convert.ToDouble(result.Split(new[] { "Cache Hit Ratio: " }, StringSplitOptions.None)[1].TrimEnd('%'));

                // Check if Cache Hit Ratio is less than 90% and print the alert.
                if (cacheHitRatio < 90)
                {
                    Console.WriteLine($"Alert Brad that cache hit ratio is {cacheHitRatio}% less than 90%");
                }
            }
        } //end of getbuffercachehitratio


    } //end of class Program

}//end of namespace
