using Oracle.DataAccess.Client;
using System;

namespace OracleDBATasks
{
    internal class UpdateActor
    {
        public static void MattsActorAddress(string destOrgActId, string srcOrgActId, string destOrgAddrId, string srcOrgAddrId)
        {
            //string destOrgActId = find out
            //string srcOrgActId =  find out   
            //string destOrgAddrId =  find out 
            //string srcOrgAddrId =  find out  

            try
            {
                using (OracleConnection connection = new OracleConnection(Daily.connectionStrings["ASTPRD"]))
                {
                    connection.Open();
                    using (OracleTransaction transaction = connection.BeginTransaction())
                    {
                        try
                        {

                            destOrgAddrId = GetOrgAddressId(connection, destOrgActId, destOrgAddrId);
                            srcOrgAddrId = GetOrgAddressId(connection, srcOrgActId, srcOrgAddrId);

                            ProcessActors(connection, destOrgActId, srcOrgActId);
                            ProcessAddresses(connection, destOrgAddrId, srcOrgAddrId);

                    
                             transaction.Commit();
                        }
                        catch (Exception ex)
                        {
                            PrintLn("Error during operation: " + ex.Message);
                            transaction.Rollback();
                            throw;
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Database connection error: " + ex.Message);
            }
        }

        static string GetOrgAddressId(OracleConnection connection, string orgActId, string orgAddrId)
        {
            if (string.IsNullOrEmpty(orgAddrId))
            {
                string query = "SELECT id FROM assurtrk.address a WHERE a.owner_id = :orgActId";
                using (OracleCommand command = new OracleCommand(query, connection))
                {
                    command.Parameters.Add(new OracleParameter("orgActId", orgActId));
                    object result = command.ExecuteScalar();
                    return result?.ToString();
                }
            }
            return orgAddrId;
        }

        static void ProcessActors(OracleConnection connection, string destOrgActId, string srcOrgActId)
        {
            DateTime startTime = DateTime.Now;
            string fkActorQuery = "SELECT a.* " +
                                  "FROM all_cons_columns a " +
                                  "JOIN all_constraints c ON a.constraint_name = c.constraint_name " +
                                  "WHERE c.constraint_type = 'R' AND r_constraint_name = 'PK_ACTOR'";
            ExecuteCursorQuery(connection, fkActorQuery, destOrgActId, srcOrgActId);
            DateTime endTime = DateTime.Now;
            TimeSpan duration = endTime - startTime;
            PrintLn($"Actor Processing Time: {duration.TotalMilliseconds} ms");
        }

        static void ProcessAddresses(OracleConnection connection, string destOrgAddrId, string srcOrgAddrId)
        {
            DateTime startTime = DateTime.Now;
            string fkAddrQuery = "SELECT a.* " +
                                 "FROM all_cons_columns a " +
                                 "JOIN all_constraints c ON a.constraint_name = c.constraint_name " +
                                 "WHERE c.constraint_type = 'R' AND r_constraint_name = 'PK_ADDRESS'";
            ExecuteCursorQuery(connection, fkAddrQuery, destOrgAddrId, srcOrgAddrId);
            DateTime endTime = DateTime.Now;
            TimeSpan duration = endTime - startTime;
            PrintLn($"Address Processing Time: {duration.TotalMilliseconds} ms");
        }

        static void ExecuteCursorQuery(OracleConnection connection, string query, string destOrgId, string srcOrgId)
        {
            using (OracleCommand command = new OracleCommand(query, connection))
            {
                using (OracleDataReader reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        string tableName = reader["table_name"].ToString();
                        string columnName = reader["column_name"].ToString();
                        ExecuteDynamicSqlForCursor(tableName, columnName, destOrgId, srcOrgId, connection);
                    }
                }
            }
        }
        static void ExecuteDynamicSqlForCursor(string tableName, string columnName, string destOrgActId, string srcOrgActId, OracleConnection connection)
        {
            string countSqlDest = BuildSql("COUNT", tableName, columnName, destOrgActId);
            int countDest = ExecuteSql(countSqlDest, connection);

            string countSqlSrc = BuildSql("COUNT", tableName, columnName, srcOrgActId);
            int countSrc = ExecuteSql(countSqlSrc, connection);

            PrintLn($"Table: {tableName}, Column: {columnName}, Dest Count: {countDest}, Src Count: {countSrc}");

            if (countDest > 0)
            {
                string updateSqlDest = BuildSql("UPDATE", tableName, columnName, destOrgActId);
                ExecuteSql(updateSqlDest, connection);
            }
            if (countSrc > 0)
            {
                string updateSqlSrc = BuildSql("UPDATE", tableName, columnName, srcOrgActId);
                ExecuteSql(updateSqlSrc, connection);
            }
        }

        static string BuildSql(string action, string tableName, string columnName, string value)
        {
            string sql = "";
            switch (action)
            {
                case "COUNT":
                    sql = $"SELECT COUNT(1) FROM {tableName} WHERE {columnName} = '{value}'";
                    break;
                case "SELECT":
                    sql = $"SELECT * FROM {tableName} WHERE {columnName} = '{value}'";
                    break;
                case "UPDATE":
                   
                    sql = $"UPDATE {tableName} SET {columnName} = 'NewValue' WHERE {columnName} = '{value}'";
                    break;
            }
            return sql;
        }

        static int ExecuteSql(string sql, OracleConnection connection)
        {
            try
            {
                using (OracleCommand command = new OracleCommand(sql, connection))
                {
                    return Convert.ToInt32(command.ExecuteScalar());
                }
            }
            catch (Exception ex)
            {
                PrintLn("Error executing command: " + sql + " | Error: " + ex.Message);
                return 0;
            }
        }

        static void PrintLn(string message)
        {
            Console.WriteLine($"{DateTime.Now:dd-MMM-yyyy HH:mm:ss} | {message}");
        }




    }
}