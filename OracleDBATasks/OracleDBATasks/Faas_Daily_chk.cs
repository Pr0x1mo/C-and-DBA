using Oracle.DataAccess.Client;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using System.Xml.Linq;
using Newtonsoft.Json.Linq;

namespace OracleDBATasks
{
    internal class Faas_Daily_chk
    {

        public static void missingIDandNamesMasterPackage()
        {
            using (OracleConnection connection = new OracleConnection(Daily.connectionStrings["ASTPRD"]))
            {
                connection.Open();
                RxNormv4(connection);

            }
        }

        public static void NDCwRxNormIdandNoName()
        {
            string query = @"select 
                                        b.ndc11 as ""b ndc11"" ,
                                        a.rx_norm_id as ""a rxnormid"",
                                        a.rxnorm_name as ""a rxnormname"", 
                                        b.rxnorm_name as ""b rxnormname"" , 
                                        b.rx_norm_id as ""b rxnormid""
                                            from faas_admin.formulary_pkg a,faas_admin.formulary_pkg b
                                           where a.latest=1 and a.completed=1 and a.owner_id='00000000-0000-0000-0000-000000000000'
                                             and b.rx_norm_id = a.rx_norm_id and b.owner_id='00000000-0000-0000-0000-000000000000' and b.latest=1
                                             and b.rxnorm_name is  null and  a.rxnorm_name is not null
                                        group by b.ndc11,a.rx_norm_id,a.rxnorm_name, b.rxnorm_name, b.rx_norm_id";

            foreach (var dbName in Daily.connectionStrings.Keys)
            {
                string connectionString = Daily.connectionStrings[dbName];
                using (OracleConnection connection = new OracleConnection(connectionString))
                {
                    connection.Open();
                    using (OracleCommand command = new OracleCommand(query, connection))
                    using (OracleDataReader reader = command.ExecuteReader())
                    {
                        if (reader.HasRows)
                        {
                            while (reader.Read())
                            {
                                string ndc11 = reader["b ndc11"].ToString();
                                string rxnormId = reader["a rxnormid"].ToString();
                                string rxnormNameA = reader["a rxnormname"].ToString();

                                UpdateRxnormName(connection, ndc11, rxnormId, rxnormNameA);
                            }
                        }
                        else
                        {
                            Console.WriteLine($"{dbName} has no issuses with NDC with RxNormId and No Name");
                        }
                    }
                    CommitTransaction(connection);
                }
            }
        }

        private static void UpdateRxnormName(OracleConnection connection, string ndc11, string rxnormId, string rxnormName)
        {
            string updateQuery = @"
                                    UPDATE faas_admin.formulary_pkg
                                    SET rxnorm_name = :rxnormName
                                    WHERE ndc11 = :ndc11
                                    AND rx_norm_id = :rxnormId
                                    AND rxnorm_name IS NULL";

            using (OracleCommand updateCommand = new OracleCommand(updateQuery, connection))
            {
                updateCommand.Parameters.Add(new OracleParameter(":rxnormName", rxnormName));
                updateCommand.Parameters.Add(new OracleParameter(":ndc11", ndc11));
                updateCommand.Parameters.Add(new OracleParameter(":rxnormId", rxnormId));

                int rowsAffected = updateCommand.ExecuteNonQuery();
                Console.WriteLine($"Updated {rowsAffected} row(s) for NDC11: {ndc11} with RXNORM ID: {rxnormId}");
            }
        }

        public static void pkgIDwIngrRxNormIdnoRxNormName () { 

            string query = @"select i.ingredient,i.ndc11, i.rx_norm_id,i.pkg_id, i.rxnorm_name
                            from faas_admin.formulary_pkg_ingr i,
                            (select pkg_id from faas_admin.formulary_pkg where owner_id='00000000-0000-0000-0000-000000000000' and latest=1) P
                            where p.pkg_id=i.pkg_id and i.rxnorm_name is  null and i.rx_norm_id is not null";
            foreach (var dbName in Daily.connectionStrings.Keys)
            {
                Console.WriteLine($"verifying {dbName}");
                string connectionString = Daily.connectionStrings[dbName];
                using (OracleConnection connection = new OracleConnection(connectionString))
                {
                    connection.Open();
                    using (OracleCommand command = new OracleCommand(query, connection))
                    using (OracleDataReader reader = command.ExecuteReader())
                    {
                        if (reader.HasRows)
                        {
                            while (reader.Read())
                            {
                                string ndc11 = reader["ndc11"].ToString();
                                string ingredient = reader["ingredient"].ToString();
                                string rxnormId = reader["rx_norm_id"].ToString();
                                Console.WriteLine($"{ndc11} ingredient is: {ingredient} missing rxnorm_name");
                                RxNormv3(ndc11, ingredient, rxnormId, connection);
                            }
                        }
                        else
                        {
                            Console.WriteLine($"{dbName} has no data");
                        }
                    }
                }
            }

        }

        public static void RxNormv3(string ndc, string ingredient, string rxnormId, OracleConnection connection)
        {
            string baseUrl = "https://rxnav.nlm.nih.gov/REST";
            using (HttpClient httpClient = new HttpClient())
            {
                JObject lookupNDC = getNDCStatus(httpClient, baseUrl, ndc);

                if (lookupNDC != null)
                {
                    string ndc11 = lookupNDC["ndc11"].ToString();
                    string rxcui = lookupNDC["rxcui"].ToString();
                    string conceptName = lookupNDC["conceptName"].ToString();

                    if (rxcui != "NO RXCUI")
                    {
                        // Use ingredient if conceptName is "NO CONCEPTNAME"
                        string finalConceptName = (conceptName == "NO CONCEPTNAME" || conceptName == "PROPRIETARY") ? ingredient : conceptName;
                        finalConceptName = finalConceptName.Replace("'", "''");

                        Console.WriteLine($@"UPDATE faas_admin.formulary_pkg_ingr
                               SET
                                   rxnorm_name = '{finalConceptName}'
                               WHERE ndc11 = '{ndc}' and rx_norm_id ='{rxnormId}';");
                        string updateSQL = $@"
                                             UPDATE faas_admin.formulary_pkg_ingr
                                             SET
                                                 rxnorm_name = '{finalConceptName}'
                                             WHERE ndc11 = '{ndc}' and rx_norm_id ='{rxnormId}'";

                        using (OracleCommand command = new OracleCommand(updateSQL, connection))
                        {
                            int rowsAffected = command.ExecuteNonQuery();
                            Console.WriteLine($"{rowsAffected} row(s) updated.");
                            CommitTransaction(connection);
                        }

                        string ndc11query = $@"select i.ingredient,i.ndc11, i.rx_norm_id,i.pkg_id, i.rxnorm_name
                                                from faas_admin.formulary_pkg_ingr i 
                                                where ndc11 = '{ndc}'";
                        using (OracleCommand command = new OracleCommand(ndc11query, connection))
                        {
                            using (OracleDataReader reader = command.ExecuteReader())
                            {
                                if (reader.HasRows)
                                {
                                    while (reader.Read())
                                    {
                                        string newingredient = reader["ingredient"].ToString();
                                        string newndc11 = reader["ndc11"].ToString();
                                        string rxnormID = reader["rx_norm_id"].ToString();
                                        string rxnormname = reader["rxnorm_name"].ToString();
                                        Console.WriteLine($"Ingredient: {newingredient}, NDC11: {newndc11}, RX_NORM_ID: {rxnormID}, RXNORM_NAME: {rxnormname} ");
                                    }
                                }
                                else
                                {
                                    Console.WriteLine("No results found.");
                                }
                            }
                        }

                    }
                }
                else
                {
                    Console.WriteLine("Error: Unable to retrieve data.");
                }

                Console.WriteLine();
            }
        }

        public static void ingrMissnigRxNormIDNotinGoldStd()
        {

            string query = @"select i.ingredient, i.rx_norm_id, i.ndc11
                            from faas_admin.formulary_pkg p,faas_admin.formulary_pkg_ingr i
                            where i.rx_norm_id is null
                                 and p.latest=1 and i.pkg_id=p.pkg_id 
                                 and p.pkg_id not in (select p.pkg_id from faas_admin.formulary_pkg p, faas_admin.formulary_pkg_ingr i, gs_admin.INGREDIENT_RXN r
                           where i.rx_norm_id is null and r.rxcui is not null
                             and p.latest=1 and i.pkg_id=p.pkg_id and i.ingredientid = r.ingredientid)
                             and i.ingredientid not in (144,691,900,1303,2119,2301,2629,3178,3238,4865,5847,6362,6363,6400)";
            foreach (var dbName in Daily.connectionStrings.Keys)
            {
                Console.WriteLine($"verifying {dbName}");
                string connectionString = Daily.connectionStrings[dbName];
                using (OracleConnection connection = new OracleConnection(connectionString))
                {
                    connection.Open();
                    using (OracleCommand command = new OracleCommand(query, connection))
                    using (OracleDataReader reader = command.ExecuteReader())
                    {
                        if (reader.HasRows)
                        {
                            while (reader.Read())
                            {
                                string ndc11 = reader["ndc11"].ToString();
                                string ingredient = reader["ingredient"].ToString();

                                Console.WriteLine($"{ndc11} ingredient is: {ingredient} missing rx_norm_id");
                                RxNormv2(ndc11, connection);
                            }
                        }
                        else
                        {
                            Console.WriteLine($"{dbName} has no data");
                        }
                    }
                }
            }
        }

        public static void ingrMissingRxnormID()
        {
            string query = @"select ndc11, product from faas_admin.formulary_pkg 
                              where owner_id='00000000-0000-0000-0000-000000000000'
                              and latest=1 and rx_norm_id is null
                              order by ndc11";

            foreach (var dbName in Daily.connectionStrings.Keys)
            {
                string connectionString = Daily.connectionStrings[dbName];
                using (OracleConnection connection = new OracleConnection(connectionString))
                {
                    connection.Open();
                    using (OracleCommand command = new OracleCommand(query, connection))
                    using (OracleDataReader reader = command.ExecuteReader())
                    {
                        if (reader.HasRows)
                        {
                            while (reader.Read())
                            {
                                string ndc11 = reader["ndc11"].ToString();
                                string ingredient = reader["product"].ToString();

                                Console.WriteLine($"{ndc11} ingredient is: {ingredient}");
                                RxNorm(ndc11, ingredient, connection);
                            }
                        }
                        else
                        {
                            Console.WriteLine($"{dbName} has no data");
                        }
                    }
                }
            }
        }// ingrMissingRxnormID()

        public static void RxNormv2(string ndc, OracleConnection connection)
        {
            string baseUrl = "https://rxnav.nlm.nih.gov/REST";
            using (HttpClient httpClient = new HttpClient())
            {
                JObject lookupNDC = getNDCStatus(httpClient, baseUrl, ndc);

                if (lookupNDC != null)
                {
                    string ndc11 = lookupNDC["ndc11"].ToString();
                    string rxcui = lookupNDC["rxcui"].ToString();
                   

                    if (rxcui != "NO RXCUI")
                    {
                        // Use ingredient if conceptName is "NO CONCEPTNAME"
                        


                        Console.WriteLine($@"UPDATE faas_admin.formulary_pkg_ingr
                               SET
                                   rx_norm_id = CASE
                                       WHEN rx_norm_id IS NULL THEN {rxcui}
                                       ELSE rx_norm_id
                                   END                                   
                               WHERE ndc11 = '{ndc}';");
                        string updateSQL = $@"
                                             UPDATE faas_admin.formulary_pkg_ingr
                                             SET
                                                 rx_norm_id = CASE
                                                     WHEN rx_norm_id IS NULL THEN {rxcui}
                                                     ELSE rx_norm_id
                                                 END                                                
                                             WHERE ndc11 = '{ndc}'";

                        using (OracleCommand command = new OracleCommand(updateSQL, connection))
                        {
                            int rowsAffected = command.ExecuteNonQuery();
                            Console.WriteLine($"{rowsAffected} row(s) updated.");
                            CommitTransaction(connection);
                        }

                        string ndc11query = $@"select i.ndc11, i.rx_norm_id, i.ingredient from faas_admin.formulary_pkg_ingr i                                           
                                               where i.ndc11 = '{ndc}'";
                        using (OracleCommand command = new OracleCommand(ndc11query, connection))
                        {
                            using (OracleDataReader reader = command.ExecuteReader())
                            {
                                if (reader.HasRows)
                                {
                                    while (reader.Read())
                                    {
                                        string retrievedNdc11 = reader["ndc11"].ToString();
                                        string retrievedRxNormId = reader["rx_norm_id"].ToString();
                                        string ingredient = reader["ingredient"].ToString();
                                        Console.WriteLine($"NDC11: {retrievedNdc11}, RX_NORM_ID was changed to: {retrievedRxNormId}, Ingredient: {ingredient}");
                                    }
                                }
                                else
                                {
                                    Console.WriteLine("No results found.");
                                }
                            }
                        }

                    }
                }
                else
                {
                    Console.WriteLine("Error: Unable to retrieve data.");
                }

                Console.WriteLine();
            }
        }
        public static void RxNorm(string ndc, string ingredient, OracleConnection connection)
        {
            string baseUrl = "https://rxnav.nlm.nih.gov/REST";
            using (HttpClient httpClient = new HttpClient())
            {
                JObject lookupNDC = getNDCStatus(httpClient, baseUrl, ndc);

                if (lookupNDC != null)
                {
                    string ndc11 = lookupNDC["ndc11"].ToString();
                    string rxcui = lookupNDC["rxcui"].ToString();
                    string conceptName = lookupNDC["conceptName"].ToString();

                    if (rxcui != "NO RXCUI")
                    {
                        // Use ingredient if conceptName is "NO CONCEPTNAME"
                        string finalConceptName = (conceptName == "NO CONCEPTNAME" || conceptName == "PROPRIETARY") ? ingredient : conceptName;
                        finalConceptName = finalConceptName.Replace("'", "''");

                        Console.WriteLine($@"UPDATE faas_admin.formulary_pkg
                               SET
                                   rx_norm_id = CASE
                                       WHEN rx_norm_id IS NULL THEN {rxcui}
                                       ELSE rx_norm_id
                                   END,
                                   rxnorm_name = '{finalConceptName}'
                               WHERE ndc11 = '{ndc}';");
                        string updateSQL = $@"
                                             UPDATE faas_admin.formulary_pkg
                                             SET
                                                 rx_norm_id = CASE
                                                     WHEN rx_norm_id IS NULL THEN {rxcui}
                                                     ELSE rx_norm_id
                                                 END,
                                                 rxnorm_name = '{finalConceptName}'
                                             WHERE ndc11 = '{ndc}'";

                        using (OracleCommand command = new OracleCommand(updateSQL, connection))
                        {
                            int rowsAffected = command.ExecuteNonQuery();
                            Console.WriteLine($"{rowsAffected} row(s) updated.");
                            CommitTransaction(connection);
                        }

                        string ndc11query = $@"select ndc11, rx_norm_id, product from faas_admin.formulary_pkg 
                          where ndc11 = '{ndc}'";
                        using (OracleCommand command = new OracleCommand(ndc11query, connection))
                        {
                            using (OracleDataReader reader = command.ExecuteReader())
                            {
                                if (reader.HasRows)
                                {
                                    while (reader.Read())
                                    {
                                        string retrievedNdc11 = reader["ndc11"].ToString();
                                        string retrievedRxNormId = reader["rx_norm_id"].ToString();
                                        string product = reader["product"].ToString();
                                        Console.WriteLine($"NDC11: {retrievedNdc11}, RX_NORM_ID: {retrievedRxNormId}, Ingredient: {product}");
                                    }
                                }
                                else
                                {
                                    Console.WriteLine("No results found.");
                                }
                            }
                        }

                    }
                }
                else
                {
                    Console.WriteLine("Error: Unable to retrieve data.");
                }

                Console.WriteLine();
            }
        }

        public static JObject getNDCStatus(HttpClient httpClient, string baseUrL, string ndc)
        {
            string url = $"{baseUrL}/ndcstatus.json?caller=RxNav&ndc={Uri.EscapeDataString(ndc)}";
            Console.WriteLine(url);
            HttpResponseMessage response = httpClient.GetAsync(url).Result;

            if (response.IsSuccessStatusCode)
            {
                string content = response.Content.ReadAsStringAsync().Result;
                JObject parsedJson = JObject.Parse(content);

                string rxcui = parsedJson.SelectToken("ndcStatus.rxcui")?.ToString() ?? string.Empty;
                string conceptName = parsedJson.SelectToken("ndcStatus.conceptName")?.ToString() ?? string.Empty;

                // Check for empty or null values
                rxcui = string.IsNullOrEmpty(rxcui) ? "NO RXCUI" : rxcui;
                conceptName = string.IsNullOrEmpty(conceptName) ? "NO CONCEPTNAME" : conceptName;

                JObject resultJson = new JObject
                {
                    ["ndc11"] = parsedJson.SelectToken("ndcStatus.ndc11"),
                    ["rxcui"] = rxcui,
                    ["conceptName"] = conceptName
                };

                return resultJson;
            }
            else
            {
                return null;
            }
        }

        public static void RxNormv4(OracleConnection connection)
        {
            int counter = 0;
            string baseUrl = "https://rxnav.nlm.nih.gov/REST";
            using (HttpClient httpClient = new HttpClient())
            {
                string SQL = @"select * from NDC_ADMIN.MASTER_PACKAGE 
                       where (rx_norm_id is null and rxnorm_name is null)
                       and data_src = 'C'
                       --and TRUNC(last_update) <> TRUNC(SYSDATE)";
                using (OracleCommand command = new OracleCommand(SQL, connection))
                {
                    using (OracleDataReader reader = command.ExecuteReader())
                    {
                        if (reader.HasRows)
                        {
                            while (reader.Read())
                            {
                                string retrievedNdc11 = reader["NDC11"].ToString();
                                Console.WriteLine($"NDC11: {retrievedNdc11}");
                                JObject lookupNDC = getNDCStatus2(httpClient, baseUrl, retrievedNdc11);
                                if (lookupNDC != null)
                                {
                                    string rxcui = lookupNDC["rxcui"]?.ToString() ?? "NULL";
                                    string conceptName = lookupNDC["conceptName"]?.ToString() ?? "NULL";
                                    conceptName = conceptName.Replace("'", "''");

                                    // Building the SET part of the SQL statement
                                    string setClause = "";
                                    if (!string.IsNullOrEmpty(conceptName) && conceptName != "NO CONCEPTNAME" && conceptName != "PROPRIETARY")
                                    {
                                        setClause = $"rxnorm_name = '{conceptName}'";
                                    }


                                    if (!string.IsNullOrEmpty(rxcui) && rxcui != "NO RXCUI")
                                    {
                                        if (!string.IsNullOrEmpty(setClause))
                                        {
                                            setClause += ", ";
                                        }
                                        setClause += $"rx_norm_id = '{rxcui}'";
                                    }

                                    if (!string.IsNullOrEmpty(setClause))
                                    {
                                        string updateSQL = $@"UPDATE NDC_ADMIN.MASTER_PACKAGE
                                                      SET {setClause}
                                                      WHERE ndc11 = '{retrievedNdc11}'";
                                        Console.WriteLine(updateSQL);
                                        using (OracleCommand updateCommand = new OracleCommand(updateSQL, connection))
                                        {
                                            int rowsAffected = updateCommand.ExecuteNonQuery();
                                            Console.WriteLine($"{rowsAffected} row(s) updated.");
                                            CommitTransaction(connection);
                                        }
                                    }
                                    else
                                    {
                                        Console.WriteLine($"Skipping NDC as no valid rxnorm_name {conceptName} or rx_norm_id {rxcui} found.");
                                        counter++;
                                    }
                                }
                            }
                        }
                        else
                        {
                            Console.WriteLine("No results found.");
                        }
                    }
                }
            }
            Console.WriteLine($"There were {counter} unknowns");
        }
        public static JObject getNDCStatus2(HttpClient httpClient, string baseUrL, string ndc)
        {
            string url = $"{baseUrL}/ndcstatus.json?caller=RxNav&ndc={Uri.EscapeDataString(ndc)}";
            Console.WriteLine(url);
            HttpResponseMessage response = httpClient.GetAsync(url).Result;

            if (response.IsSuccessStatusCode)
            {
                string content = response.Content.ReadAsStringAsync().Result;
                JObject parsedJson = JObject.Parse(content);

                string rxcui = parsedJson.SelectToken("ndcStatus.rxcui")?.ToString() ?? string.Empty;
                string conceptName = parsedJson.SelectToken("ndcStatus.conceptName")?.ToString() ?? string.Empty;

                rxcui = string.IsNullOrEmpty(rxcui) ? "NO RXCUI" : rxcui;
                conceptName = string.IsNullOrEmpty(conceptName) ? "NO CONCEPTNAME" : conceptName;

                JObject resultJson = new JObject
                {
                    ["ndc11"] = parsedJson.SelectToken("ndcStatus.ndc11"),
                    ["rxcui"] = rxcui,
                    ["conceptName"] = conceptName
                };

                JArray sourceList = (JArray)parsedJson.SelectToken("ndcStatus.sourceList.sourceName");
                List<string> sourceNames = sourceList?.ToObject<List<string>>() ?? new List<string>();
                resultJson["sourceNames"] = new JArray(sourceNames);

                return resultJson;
            }
            else
            {
                return null;
            }
        }
    
    public static void ndc_labelers()
        {

            foreach (var dbName in Daily.connectionStrings.Keys)
            {
                var connectionString = Daily.connectionStrings[dbName];
                using (OracleConnection connection = new OracleConnection(connectionString))
                {
                    connection.Open();

                    List<string> labelerCodes = LabelerCodeExists(connection);
                    if (labelerCodes.Count > 0)
                    {
                        foreach (var labelerCode in labelerCodes)
                        {
                            InsertLabeler(connection, labelerCode);
                        }
                        CommitTransaction(connection);
                    }
                    else
                    {
                        Console.WriteLine($"All GS labelers are in NDC labelers for {dbName}");
                    }
                }
            }

        }

        public static List<string> LabelerCodeExists(OracleConnection connection)
        {
            string query = "select labelercode from gs_admin.company " +
                           "where labelercode not in (select hipaa_code from ndc_admin.ndc_labelers) " +
                           "group by labelercode";

            var labelerCodes = new List<string>();
            using (OracleCommand command = new OracleCommand(query, connection))
            {
                using (OracleDataReader reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        labelerCodes.Add(reader["labelercode"].ToString());
                    }
                }
            }

            return labelerCodes;
        }

        public static void InsertLabeler(OracleConnection connection, string labelerCode)
        {
            string query = "INSERT INTO ndc_admin.ndc_labelers (labeler_code, hipaa_code, labeler, labeler_short, data_src) " +
                           "SELECT TO_CHAR(labelercode), LPAD(TO_CHAR(labelercode), 5, '0'), companyname, companynameshort, 'G' " +
                           "FROM gs_admin.company WHERE labelercode = :labelerCode";

            using (OracleCommand command = new OracleCommand(query, connection))
            {
                command.Parameters.Add(new OracleParameter(":labelerCode", labelerCode));
                Console.WriteLine($"inserting labelerCode {labelerCode}");
                command.ExecuteNonQuery();
            }
        }

        public static void CommitTransaction(OracleConnection connection)
        {
            string query = "COMMIT";
            using (OracleCommand command = new OracleCommand(query, connection))
            {
                command.ExecuteNonQuery();
            }
        }
    }
}