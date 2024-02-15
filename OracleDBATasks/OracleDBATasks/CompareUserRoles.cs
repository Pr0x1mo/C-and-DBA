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
    internal class CompareUserRoles
    {
        // Assuming connectionStrings are defined elsewhere in your class.
        public static void CompareUsers() {

            //table or view here
            //CompareUsersForView("NDC_LOOKUPS");
           // CompareUsersForView("MASTER_PACKAGE");
            //CompareUsersForView("MASTER_PRODUCT");
            CompareUsersForView("Actor");
            //CompareUsersForView("ADDRESS");
            //CompareUsersForView("SERIALNUMBERLOCATION");


        }
        public static void CompareUsersForView(string viewName)


        {
            //ASTPRD ASTDEV ASTDEMO ASTSIT
            string dbName1 = "ASTPRD";
            string dbName2 = "CTXRPT";
            var astprdUserRolesForView = GetUserRolesForView(Daily.connectionStrings[dbName1], viewName);
            var astdemoUserRolesForView = GetUserRolesForView(Daily.connectionStrings[dbName2], viewName);
            Console.WriteLine(viewName);
            Console.WriteLine("{0,-30} {1,-50} {2,-50} {3}", "USER", $"{dbName1} PRIVILEGES", $"{dbName2} PRIVILEGES", "DIFFERENCE?");
            Console.WriteLine(new string('-', 140));

            // Assuming that all users should exist in both databases
            var allUsers = astprdUserRolesForView.Keys.Union(astdemoUserRolesForView.Keys).OrderBy(u => u);

            foreach (var user in allUsers)
            {
                var prodPrivileges = astprdUserRolesForView.ContainsKey(user) ? string.Join(", ", astprdUserRolesForView[user]) : "No Privileges";
                var demoPrivileges = astdemoUserRolesForView.ContainsKey(user) ? string.Join(", ", astdemoUserRolesForView[user]) : "No Privileges";

                bool isDifferent = prodPrivileges != demoPrivileges;
                string difference = isDifferent ? "Yes" : "No";

                Console.WriteLine("{0,-30} {1,-50} {2,-50} {3}", user, prodPrivileges, demoPrivileges, difference);
            }

            Console.WriteLine("Comparison of view/table privileges completed.");
            Console.WriteLine();
        }


  
        private static Dictionary<string, List<string>> GetUserRolesForView(string connectionString, string viewName = null)
        {
            var userRolesForView = new Dictionary<string, List<string>>();

            string query = @"
                            SELECT 
                                GRANTEE, 
                                PRIVILEGE,
                                TABLE_NAME
                            FROM 
                                DBA_TAB_PRIVS";
                           //WHERE GRANTEE LIKE 'ASSUR%'";

            // Modify query if a specific view or table name is provided
            if (!string.IsNullOrEmpty(viewName))
            {
                query += " WHERE TABLE_NAME = :viewName";  //change this back to AND TABLE_NAME if you uncomment the where above 
            }


            using (var conn = new OracleConnection(connectionString))
            {
                conn.Open();
                using (var command = new OracleCommand(query, conn))
                {
                    if (!string.IsNullOrEmpty(viewName))
                    {
                        command.Parameters.Add(new OracleParameter("viewName", viewName.ToUpper()));
                    }


                    using (var reader = command.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            string user = reader["GRANTEE"].ToString();
                            string privilege = reader["PRIVILEGE"].ToString();
                            string tableName = reader["TABLE_NAME"].ToString();

                            string key = string.IsNullOrEmpty(viewName) ? $"{user}|{tableName}" : user;

                            if (!userRolesForView.ContainsKey(key))
                            {
                                userRolesForView[key] = new List<string>();
                            }
                            userRolesForView[key].Add(privilege);
                        }
                    }
                }
            }

            return userRolesForView;
        }
    }
}