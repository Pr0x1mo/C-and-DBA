using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Oracle.DataAccess.Client;


namespace OracleDBATasks
{
    internal class Create_Index
    {
        public static void CreateIndex()
        {
            string owner = "ASSURTRK";
            string indexName = "PRODORDBTCH_LU_IDX";
            string tableName = "PRODITEMBATCH";
            string frmDB = "ASTPRD";
            string toDb = "ASTDEV";
            // Fetch index and column details from the db that has it
            string demoConnectionString = Daily.connectionStrings[frmDB];
            string columnName = GetIndexColumnName(demoConnectionString, owner, tableName, indexName);

            // Construct and execute CREATE INDEX on the db that doesn't have it 

            string prdConnectionString = Daily.connectionStrings[toDb];
            ExecuteCreateIndex(prdConnectionString, owner, tableName, indexName, columnName);
        }

        private static string GetIndexColumnName(string connectionString, string owner, string tableName, string indexName)
        {
            string query = @"SELECT aic.column_name
                            FROM all_indexes ai
                            JOIN all_ind_columns aic ON ai.index_name = aic.index_name
                            WHERE ai.table_name = :tableName AND ai.owner = :owner AND ai.index_name = :indexName";

            using (OracleConnection conn = new OracleConnection(connectionString))
            {
                conn.Open();
                using (OracleCommand cmd = new OracleCommand(query, conn))
                {
                    cmd.Parameters.Add(new OracleParameter("tableName", tableName));
                    cmd.Parameters.Add(new OracleParameter("owner", owner));
                    cmd.Parameters.Add(new OracleParameter("indexName", indexName));

                    using (OracleDataReader reader = cmd.ExecuteReader())
                    {
                        if (reader.Read())
                        {
                            string columnName = reader["column_name"].ToString();
                            Console.WriteLine($"{owner} {indexName} {owner} {tableName} {columnName}");
                            return reader["column_name"].ToString();
                           
                        }
                    }
                }
            }
            return null; 
        }

        private static void ExecuteCreateIndex(string connectionString, string owner, string tableName, string indexName, string columnName)
        {
            string createIndexQuery = $"CREATE INDEX \"{owner}\".\"{indexName}\" ON \"{owner}\".\"{tableName}\" (\"{columnName}\")";
            string fetchIndexQuery = @"SELECT ai.owner, ai.index_name, ai.table_name, aic.column_name
                               FROM all_indexes ai
                               JOIN all_ind_columns aic ON ai.index_name = aic.index_name
                               WHERE ai.table_name = :tableName AND ai.owner = :owner AND aic.column_name = :columnName";

            using (OracleConnection conn = new OracleConnection(connectionString))
            {
                try
                {
                    conn.Open();
                    using (OracleCommand cmd = new OracleCommand(createIndexQuery, conn))
                    {
                        cmd.ExecuteNonQuery();
                        Console.WriteLine($"Index {indexName} created successfully on table {tableName}.");
                        foreach (var pair in Daily.connectionStrings)
                        {
                            Console.WriteLine($"{pair.Key}");
                            FetchAndPrintIndexDetails(pair.Value, owner, tableName, indexName, columnName, fetchIndexQuery);
                        }
                    }
                }
                catch (Oracle.DataAccess.Client.OracleException ex)
                {
                    Console.WriteLine($"Oracle database error occurred: {ex.ErrorCode} - {ex.Message}");

                    // Run the comparison query on each database
                    foreach (var pair in Daily.connectionStrings)
                    {
                        Console.WriteLine($"{pair.Key}");
                        FetchAndPrintIndexDetails(pair.Value, owner, tableName, indexName, columnName, fetchIndexQuery);
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"An error occurred: {ex.Message}");
                }
            }
        }

        private static void FetchAndPrintIndexDetails(string connectionString, string owner, string tableName, string indexName, string columnName, string query)
        {
            using (OracleConnection conn = new OracleConnection(connectionString))
            {
                conn.Open();
                using (OracleCommand cmd = new OracleCommand(query, conn))
                {
                    cmd.Parameters.Add(new OracleParameter("tableName", tableName));
                    cmd.Parameters.Add(new OracleParameter("owner", owner));
                    cmd.Parameters.Add(new OracleParameter("columnName", columnName));

                    using (OracleDataReader reader = cmd.ExecuteReader())
                    {
                        // Define column widths
                        int ownerWidth = 20;
                        int indexNameWidth = 30;
                        int tableNameWidth = 30;
                        int columnNameWidth = 30;

                        // Print the headers
                        //Console.WriteLine("Fetching index details from database: " + connectionString.Split(';')[0]);
                        Console.WriteLine(String.Format("{0,-" + ownerWidth + "} | {1,-" + indexNameWidth + "} | {2,-" + tableNameWidth + "} | {3,-" + columnNameWidth + "}",
                                                        "OWNER", "INDEX_NAME", "TABLE_NAME", "COLUMN_NAME"));
                        Console.WriteLine(new string('-', ownerWidth + indexNameWidth + tableNameWidth + columnNameWidth + 9)); // Adjust the total length

                        while (reader.Read())
                        {
                            string fetchedOwner = reader.GetString(0);
                            string fetchedIndexName = reader.GetString(1);
                            string fetchedTableName = reader.GetString(2);
                            string fetchedColumnName = reader.GetString(3);

                            // Print the index details
                            Console.WriteLine(String.Format("{0,-" + ownerWidth + "} | {1,-" + indexNameWidth + "} | {2,-" + tableNameWidth + "} | {3,-" + columnNameWidth + "}",
                                                            fetchedOwner, fetchedIndexName, fetchedTableName, fetchedColumnName));
                        }

                        Console.WriteLine(new string('-', ownerWidth + indexNameWidth + tableNameWidth + columnNameWidth + 9)); // Separator line
                    }
                }
            }
        }

    }
}
