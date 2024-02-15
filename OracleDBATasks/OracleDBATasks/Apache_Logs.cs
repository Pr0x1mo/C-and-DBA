using Oracle.DataAccess.Client;
using Renci.SshNet;
using Renci.SshNet.Sftp;
using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace OracleDBATasks
{
    internal class Apache_Logs
    {
        public static void Apache_Log_Getter()
        {
            string[] ipAddresses = { "10.1.1.50", "10.1.1.219", "10.1.1.161", "10.1.1.182" };

            string basePath = "/logs/*/apache/";
            string datePattern = "2023-10-26";// "yyyy-MM-dd";
            string path = basePath + "ssl_request_log-" + DateTime.Now.ToString(datePattern);
            string privateKeyFilePath = @"C:/Users/XavierBorja/Documents/putty/opc_ssh";
            var keyFile = new PrivateKeyFile(privateKeyFilePath);
            string localPath = $@"C:\Users\XavierBorja\Desktop\ssl_request_log_All\{DateTime.Now.ToString(datePattern)}.log";
            Directory.CreateDirectory(Path.GetDirectoryName(localPath));

            using (StreamWriter writer = new StreamWriter(localPath, false))
            {

                string[] columnHeaders = {
                    "Time/Date",
                    "Oracle Server IP",
                    "Load Balancer IP",
                    "User IP",
                    "Remote Host Name",
                    "Service Request",
                    "ms Value"
                };

                writer.WriteLine(string.Join(",", columnHeaders));

                foreach (var ipAddress in ipAddresses)
                {
                    using (var client = new SshClient(ipAddress, "opc", keyFile))
                    {
                        client.Connect();
                        Console.WriteLine($"Fetching logs from {ipAddress}: {path}");

                        var cmd = client.CreateCommand($"cat {path}");
                        var asyncResult = cmd.BeginExecute();

                        using (var reader = new StreamReader(cmd.OutputStream, Encoding.UTF8, true, 4096, true))
                        {
                            while (!asyncResult.IsCompleted || !reader.EndOfStream)
                            {
                                var line = reader.ReadLine();
                                if (line != null)
                                {
                                    var columns = line.Split(new string[] { " - " }, StringSplitOptions.None).Select(col => col.Trim()).ToArray();
                                    for (int i = 0; i < columns.Length; i++)
                                    {
                                        columns[i] = columns[i].Replace(",", " ");
                                    }
                                    string msValue = string.Empty;
                                    for (int i = 4; i < columns.Length; i++)
                                    {
                                        var match = Regex.Match(columns[i], @"-ms\s*:\s*(\d+)");
                                        if (match.Success)
                                        {
                                            msValue = match.Groups[1].Value;
                                            break;
                                        }
                                    }


                                    if (columns.Length >= 5
                                 && !string.IsNullOrWhiteSpace(columns[1]) && columns[1] != "-"
                                 && !string.IsNullOrWhiteSpace(columns[2]) && columns[2] != "-"
                                 && !string.IsNullOrWhiteSpace(columns[3]) && columns[3] != "-"
                                 && !columns[4].Contains("js") && !columns[4].Contains("ws") && !columns[4].Contains("css")
                                 && columns[4].Contains("GET /patient/production/assembly?"))
                                    {
                                        var ips = ExtractIPs(line);
                                        string oracleServerIP = ips.Count > 0 ? ips[0] : "";
                                        string loadBalancerIP = "";
                                        string userIP = "";

                                        if (ips.Count == 3)
                                        {
                                            loadBalancerIP = ips[1];
                                            userIP = ips[2];
                                        }
                                        else if (ips.Count == 2)
                                        {
                                            userIP = ips[1];
                                        }

                                        string remoteHostName = columns[3];
                                        string serviceRequest = columns[4];
                                       //int parsedMsValue = int.Parse(msValue);
                                       // {
                                       // parsedMsValue = 0;  // default value or any other handling logic                                    
                                       // Console.WriteLine($"Warning: Could not parse -ms value '{msValue}' from line: {line}");
                                       //}
                                        string row = $"{columns[0]},{oracleServerIP},{loadBalancerIP},{userIP},{remoteHostName},{serviceRequest},{msValue}";
                                       // Console.WriteLine($"{columns[0]} {oracleServerIP} {loadBalancerIP} {userIP} {remoteHostName} {serviceRequest} {msValue}");                                                                      
                                       // Console.WriteLine(row);
                                          writer.WriteLine(row);
                                    }
                                }
                            }
                        }

                        cmd.EndExecute(asyncResult);
                        client.Disconnect();
                    }
                }
            }
            List<string> allRows = File.ReadAllLines(localPath).Skip(1).ToList(); // Skipping header
            InsertIntoOracle(allRows);
            Process.Start("notepad++.exe", localPath);
            Console.WriteLine();
        }

        public static List<string> ExtractIPs(string line)
        {
            var ipPattern = @"\b\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\b";
            var matches = Regex.Matches(line, ipPattern);
            return matches.Cast<Match>().Select(m => m.Value).ToList();
        }
  
    public static void InsertIntoOracle(List<string> rows)
    {
        using (OracleConnection conn = new OracleConnection("Data Source=(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)(HOST=10.1.1.213)(PORT=1521)))(CONNECT_DATA=(SERVER=DEDICATED)(SERVICE_NAME=astprd_pdb1.prodprisubphx.consortiexpxvcn.oraclevcn.com)));User Id=xborja;Password=Seraphim24$"))
        {
            conn.Open();

            // Check if table exists, if not, create it
            using (OracleCommand cmd = new OracleCommand())
            {
                cmd.Connection = conn;
                cmd.CommandText = @"
            DECLARE
               count_table NUMBER;
            BEGIN
               SELECT COUNT(*)
               INTO   count_table
               FROM   user_tables
               WHERE  table_name = 'APACHE_LOGS';

               IF count_table = 0 THEN
                  EXECUTE IMMEDIATE 'CREATE TABLE APACHE_LOGS
                  (
                     TIMEDATE VARCHAR2(4000),
                     ORACLESERVERIP VARCHAR2(4000),
                     LOADBALANCERIP VARCHAR2(4000),
                     USERIP VARCHAR2(4000),
                     REMOTEHOST VARCHAR2(4000),
                     SERVICE1 VARCHAR2(4000),
                     MSVALUE NUMBER(10)
                  )';
               END IF;
            END;";
                cmd.ExecuteNonQuery();
            }

            // Insert fetched data into Oracle
            foreach (var row in rows)
            {
                using (OracleCommand cmd = new OracleCommand())
                {
                    cmd.Connection = conn;
                    cmd.CommandText = "INSERT INTO APACHE_LOGS(TIMEDATE, ORACLESERVERIP, LOADBALANCERIP, USERIP, REMOTEHOST, SERVICE1, MSVALUE) VALUES (:1, :2, :3, :4, :5, :6, :7)";
                    var columns = row.Split(',');
                    cmd.Parameters.Add(new OracleParameter("1", columns[0]));
                    cmd.Parameters.Add(new OracleParameter("2", columns[1]));
                    cmd.Parameters.Add(new OracleParameter("3", columns[2]));
                    cmd.Parameters.Add(new OracleParameter("4", columns[3]));
                    cmd.Parameters.Add(new OracleParameter("5", columns[4]));
                    cmd.Parameters.Add(new OracleParameter("6", columns[5]));
                    cmd.Parameters.Add(new OracleParameter("7", columns[6]));

                    cmd.ExecuteNonQuery();
                }
            }

            conn.Close();
        }
        }
    }
}


//namespace OracleDBATasks
//{
//    internal class Apache_Logs
//    {
//        public static void Apache_Log_Getter()
//        {
//            string[] ipAddresses = { "10.1.1.50"   , "10.1.1.219", "10.1.1.161", "10.1.1.182" };
//        //;//

//        string basePath = "/logs/*/apache/";
//            string datePattern = "yyyy-MM-dd";
//            string path = basePath + "ssl_request_log-" + DateTime.Now.ToString(datePattern);
//            string privateKeyFilePath = @"C:/Users/XavierBorja/Documents/putty/opc_ssh";
//            var keyFile = new PrivateKeyFile(privateKeyFilePath);
//            string localPath = $@"C:\Users\XavierBorja\Desktop\ssl_request_log_All\{DateTime.Now.ToString(datePattern)}.log";
//            Directory.CreateDirectory(Path.GetDirectoryName(localPath));
//            DataTable table = new DataTable();
//            table.Columns.Add("Time/Date");
//            table.Columns.Add("Oracle Server IP");
//            table.Columns.Add("Load Balancer IP");
//            table.Columns.Add("User IP");
//            table.Columns.Add("Remote Host Name");
//            table.Columns.Add("Service Request");
//            table.Columns.Add("-ms Value", typeof(int));   // New column for -ms values

//            foreach (var ipAddress in ipAddresses)
//            {
//                using (var client = new SshClient(ipAddress, "opc", keyFile))
//                {
//                    client.Connect();
//                    Console.WriteLine($"Fetching logs from {ipAddress}: {path}");

//                    using (var cmd = client.CreateCommand($"cat {path}"))
//                    {
//                        cmd.Execute();
//                        string output = cmd.Result;

//                        var lines = output.Split('\n');


//                        foreach (var line in lines)
//                        {
//                            var columns = line.Split(new string[] { " - " }, StringSplitOptions.None).Select(col => col.Trim()).ToArray();

//                            string msValue = string.Empty;
//                            for (int i = 4; i < columns.Length; i++)
//                            {
//                                var match = Regex.Match(columns[i], @"-ms\s*:\s*(\d+)");
//                                if (match.Success)
//                                {
//                                    msValue = match.Groups[1].Value;
//                                    break;
//                                }
//                            }

//                            if (columns.Length >= 5
//                                && !string.IsNullOrWhiteSpace(columns[1]) && columns[1] != "-"
//                                && !string.IsNullOrWhiteSpace(columns[2]) && columns[2] != "-"
//                                && !string.IsNullOrWhiteSpace(columns[3]) && columns[3] != "-"
//                                && !columns[4].Contains("js") && !columns[4].Contains("ws") && !columns[4].Contains("css"))
//                            {
//                                var ips = ExtractIPs(line);
//                                string oracleServerIP = ips.Count > 0 ? ips[0] : "";
//                                string loadBalancerIP = "";
//                                string userIP = "";

//                                if (ips.Count == 3)
//                                {
//                                    loadBalancerIP = ips[1];
//                                    userIP = ips[2];
//                                }
//                                else if (ips.Count == 2)
//                                {
//                                    userIP = ips[1];
//                                }

//                                string remoteHostName = columns[3];
//                                string serviceRequest = columns[4];

//                                int parsedMsValue = int.Parse(msValue);
//                                table.Rows.Add(columns[0], oracleServerIP, loadBalancerIP, userIP, remoteHostName, serviceRequest, parsedMsValue);


//                                //Console.WriteLine($"{columns[0]} {oracleServerIP} {loadBalancerIP} {userIP} {remoteHostName} {serviceRequest} {parsedMsValue}");
//                            }
//                        }


//                    }
//                    client.Disconnect();
//                }
//            }


//            //// Calculate the maximum width for each column
//            if (table.Rows.Count > 0)
//            {

//                DataView dv = table.DefaultView;
//                dv.Sort = "-ms Value DESC";  // Sort the "ms Value" column in descending order
//                DataTable sortedTable = dv.ToTable();

//                Console.WriteLine($"table row count {table.Rows.Count}");

//                using (StreamWriter writer = new StreamWriter(localPath, false))
//                {
//                    // Writing Column Names
//                    for (int i = 0; i < sortedTable.Columns.Count; i++)
//                    {
//                        writer.Write(sortedTable.Columns[i].ColumnName);
//                        if (i < sortedTable.Columns.Count - 1) // To avoid adding space after the last column
//                        {
//                            writer.Write(",");
//                        }
//                    }
//                    writer.WriteLine();

//                    // Printing Column Names to Console
//                    for (int i = 0; i < sortedTable.Columns.Count; i++)
//                    {
//                        //  Console.Write(sortedTable.Columns[i].ColumnName);
//                        if (i < sortedTable.Columns.Count - 1) // To avoid adding space after the last column
//                        {
//                            Console.Write(",");
//                        }
//                    }
//                    Console.WriteLine();

//                    // Writing Rows
//                    foreach (DataRow row in sortedTable.Rows)
//                    {
//                        for (int i = 0; i < sortedTable.Columns.Count; i++)
//                        {
//                            string trimmedData = row[i].ToString().Trim();
//                            writer.Write(trimmedData);
//                            if (i < sortedTable.Columns.Count - 1) // To avoid adding space after the last column
//                            {
//                                writer.Write(",");
//                            }
//                        }
//                        writer.WriteLine();
//                    }
//                }
//            }
//            else
//            {
//                Console.WriteLine("No data to write.");
//            }
//            Process.Start("notepad++.exe", localPath);
//            Console.WriteLine();
//        }

//        public static List<string> ExtractIPs(string line)
//        {
//            var ipPattern = @"\b\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\b";
//            var matches = Regex.Matches(line, ipPattern);
//            return matches.Cast<Match>().Select(m => m.Value).ToList();
//        }
//    }
//}
