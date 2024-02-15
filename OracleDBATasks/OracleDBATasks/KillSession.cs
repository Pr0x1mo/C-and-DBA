using Oracle.DataAccess.Client;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace OracleDBATasks
{
    internal class KillSession
    {
        public static void TerminateMySessions()
        {
            string username = "XBORJA"; // Replace with your Oracle username

            foreach (string dbName in Daily.connectionStrings.Keys)
            {
                Console.WriteLine($"Killing session for myself in {dbName}");
                string connectionString = Daily.connectionStrings[dbName];

                using (OracleConnection connection = new OracleConnection(connectionString))
                {
                    connection.Open();

                    string query = "SELECT s.sid, s.serial#, p.spid, s.username, s.schemaname, s.program, s.terminal, s.osuser, q.sql_text, " +
                                   "ROUND(s.last_call_et/60, 2) AS query_duration_minutes " +
                                   "FROM v$session s " +
                                   "JOIN v$process p ON s.paddr = p.addr " +
                                   "LEFT JOIN v$sql q ON s.sql_id = q.sql_id " +
                                   $"WHERE s.username = '{username}'";

                    using (OracleCommand command = new OracleCommand(query, connection))
                    using (OracleDataReader reader = command.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            int sid = Convert.ToInt32(reader["sid"]);
                            int serial = Convert.ToInt32(reader["serial#"]);

                            if (SessionExists(connectionString, sid, serial))
                            {
                                string killCommand = $"ALTER SYSTEM KILL SESSION '{sid},{serial}' immediate";
                                Console.WriteLine($"Killing {username}, sid: {sid}, serial#: {serial}");
                                ExecuteKillCommand(connectionString, killCommand);
                            }
                            else
                            {
                                Console.WriteLine($"Session {sid}, serial#: {serial} does not exist.");
                            }
                        }
                    }
                }
            }
        }

        public static bool SessionExists(string connectionString, int sid, int serial)
        {
            using (OracleConnection connection = new OracleConnection(connectionString))
            {
                connection.Open();
                string query = "SELECT 1 FROM v$session WHERE sid = :sid AND serial# = :serial";

                using (OracleCommand command = new OracleCommand(query, connection))
                {
                    command.Parameters.Add("sid", OracleDbType.Int32).Value = sid;
                    command.Parameters.Add("serial", OracleDbType.Int32).Value = serial;

                    object result = command.ExecuteScalar();
                    return (result != null && result != DBNull.Value);
                }
            }
        }

        public static void ExecuteKillCommand(string connectionString, string killCommand)
        {
            using (OracleConnection connection = new OracleConnection(connectionString))
            {
                connection.Open();
                using (OracleCommand command = new OracleCommand(killCommand, connection))
                {
                    command.ExecuteNonQuery();
                }
            }
        }

    }
}
