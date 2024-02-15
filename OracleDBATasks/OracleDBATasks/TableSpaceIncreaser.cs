using System;
using System.Collections.Generic;
using Oracle.DataAccess.Client;

namespace OracleDBATasks
{
    internal class TableSpaceIncreaser
    {
        public static void TableSpaceIncrease()
        {
            foreach (var dbName in Daily.connectionStrings.Keys)
            {
                var connectionString = Daily.connectionStrings[dbName];

                using (var connection = new OracleConnection(connectionString))
                {
                    connection.Open();
                    string query = "select a.tablespace_name tbs," +
                        "a.bytes_alloc/(1024*1024) " +
                        "alloc,round(nvl(b.tot_used,0)/(1024*1024)) used,\r\n      " +
                        "round((nvl(b.tot_used,0)/a.bytes_alloc)*100,2) pct,\r\n      " +
                        "a.bytes_alloc/(1024*1024)-round(nvl(b.tot_used,0)/(1024*1024)) " +
                        "free\r\nfrom (select tablespace_name,sum(bytes) physical_bytes,\r\n     " +
                        "sum(decode(autoextensible,'NO',bytes,'YES',maxbytes)) bytes_alloc\r\nfrom dba_data_files\r\n         " +
                        "group by tablespace_name) a,\r\n         " +
                        "(select tablespace_name,sum(bytes) tot_used\r\n            " +
                        "from dba_segments\r\n          " +
                        "group by tablespace_name) b\r\n   " +
                        "where a.tablespace_name = b.tablespace_name (+)\r\n     " +
                        "and (nvl(b.tot_used,0)/a.bytes_alloc)*100 > 80\r\n    " +
                        "and a.bytes_alloc/(1024*1024)-round(nvl(b.tot_used,0)/(1024*1024)) < 4096\r\n     " +
                        "and a.tablespace_name not in (select distinct tablespace_name from dba_temp_files)\r\n     " +
                        "and a.tablespace_name not like 'UNDO%'\r\n  " +
                        "order by 1";
                    using (OracleCommand command = new OracleCommand(query, connection))
                    {
                        OracleDataReader reader = command.ExecuteReader();
                        bool anyRowsFound = false;

                        while (reader.Read())
                        {
                            anyRowsFound = true;
                            string tablespaceName = reader["tbs"].ToString();
                            double usedPercent = Convert.ToDouble(reader["pct"]);
                            int freeMb = Convert.ToInt32(reader["free"]);
                            Console.WriteLine($"{dbName} {tablespaceName} {usedPercent:F2} {freeMb}");

                            // Check the criteria and then run the increase tablespace command
                            if (usedPercent > 80 && freeMb < 4096)
                            {
                                IncreaseTableSpace(connection, tablespaceName);
                            }
                        }
                        if (!anyRowsFound)
                        {
                            Console.WriteLine($"{dbName}: Database is good.");
                        }
                    }
                }
                Console.WriteLine("Press Enter for Next Database");
                Console.ReadLine();
            }
        }

        private static void IncreaseTableSpace(OracleConnection connection, string tablespaceName)
        {
            string increaseTablespaceCommand = $"ALTER TABLESPACE \"{tablespaceName}\" ADD DATAFILE '+DATA' SIZE 16G AUTOEXTEND ON NEXT 1G MAXSIZE 33554416K";

            using (OracleCommand command = new OracleCommand(increaseTablespaceCommand, connection))
            {
                command.ExecuteNonQuery();
                Console.WriteLine($"Tablespace {tablespaceName} has been increased.");
            }
        }
    }
}
