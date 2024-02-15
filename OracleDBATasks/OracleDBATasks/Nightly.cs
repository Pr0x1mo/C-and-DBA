using Oracle.DataAccess.Client;
using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Diagnostics.Eventing.Reader;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.Remoting.Messaging;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;
using System.Xml.Linq;

namespace OracleDBATasks
{
    internal class Nightly
    {
        public static void Nightly_Tasks()
        {
            // task 1 rebuild indexes
            //AnalyzeAndRebuildIndexes();
            //// task 2 gather schema stats here
            //GatherSchemaStats();
            ////continuation of Task 2 complete task here of GatherTableStats();
            //GatherTableStats();
            // task 3 Check the tablespace for respective Tables &Indexes
            //select owner, segment_name, segment_type, tablespace_name from dba_segments where owner = 'OWNER_NAME' order by segment_type;
            //EnsureIndexesInSeparateTablespaces();
            //Note: Check all the index and table are on separate tablespace. Follow the move_index_seprate_tablespace document to place the index on
            //separate tablespace.Do this activity in the night.
            ShouldIndexBeDropped();



        }

        public static void ShouldIndexBeDropped()
        {

            string[] indexNames = { "ADDR_LU_IX",
                                    "ADDRESS_OWNER_IDX",
                                    "BATING_LU_IX",
                                    "CSPTSK_LU_IX",
                                    "CSPYLD_LU_IX",
                                    "DATAENTJOB_LU_IX",
                                    "DATAENTRYACT_WHEN_IX",
                                    "DATAENTRYJOB_AFFILIATE_IX",
                                    "DATENTJOBACT_DBTS_IX",
                                    "DSCSA_DOC_IX",
                                    "DSCSA_SHPMNT_TS_IX",
                                    "DSCSA_TRANSX_APPRV_IX",
                                    "DSCSA_TRANSX_CRT_IX",
                                    "DSCSA_TRANSX_DSCSA_IX",
                                    "DSCSA_TRANSX_FROM_IX",
                                    "DSCSA_TRANSX_TO_IX",
                                    "DSCSA_TRANSX_TS_IX",
                                    "DSCSA_TRANSX_TYPE_IX",
                                    "DSCSADOC_LU_IX",
                                    "DSCSTX_IDX_INV_PO_AND_MORE",
                                    "DSCSX_INV_FI",
                                    "DSCSX_LOT_FI",
                                    "DSCSX_PO_FI",
                                    "DSCTX_INV_IX",
                                    "DSCTX_LOT_IX",
                                    "DSCTX_MARK_IX",
                                    "DSCTX_NDC_IX",
                                    "DSCTX_PO_IX",
                                    "DSCTX_POX_IX",
                                    "FDA_IDX",
                                    "FP_FDANDC_IX",
                                    "FP_NDC10_IX",
                                    "FP_NDC11_IX",
                                    "FP_PKGCODE_IX",
                                    "FPI_INGR_IX",
                                    "FPI_NDC_IX",
                                    "FPI_PKGID_IX",
                                    "FRMPKG_RXN_IDX",
                                    "GSMAX_IX",
                                    "GSMAX_PKG_IX",
                                    "HOOD_LU_IX",
                                    "ITEM_CODE_FX",
                                    "JOBACTIVITY_DOC_IX",
                                    "JOBACTIVITY_OBJECT_IX",
                                    "MPKG_ITEM_IX",
                                    "MPKG_NDC_IDX",
                                    "MPRD_LBLR_IX",
                                    "MPRD_STRN_IX",
                                    "MPRD_UOM_IX",
                                    "MSTPKG_LU_IX",
                                    "MSTPKG_NDC_IX",
                                    "MSTPKG_RXN_IX",
                                    "MSTPRD_LU_IX",
                                    "NDC_PRD_IDX",
                                    "NDC_UPD_FX",
                                    "NDCLKP_LU_IX",
                                    "OWNERBUYER_TO_IX",
                                    "OWNERSELL_FROM_IX",
                                    "PACKINGSLIP_SHIPTO_IDX",
                                    "PCKITEM_LU_IX",
                                    "PCKSLIP_LU_IX",
                                    "PEDIGREEBATCH_BATCH_IDX",
                                    "PEDIGREEBATCH_DOC_IX",
                                    "PEGBAT_LU_IX",
                                    "PKG_NDCPRD_IX",
                                    "PKGSLIPPRODITEM_BATCH_IDX",
                                    "PRDCT_NPNAME",
                                    "PRDCT_PNAME",
                                    "RXNSAT_ATV_IDX",
                                    "RXNSAT_AUI_IX",
                                    "RXNSAT_CUI_IX",
                                    "RXNSAT_RXCUI_IDX",
                                    "TASK_CSP_IDX"};

            string query = @"WITH total_executions_data AS (
                                                SELECT COUNT(*) AS total_executions
                                                FROM v$sql
                                            ), index_usage_data AS (
                                                SELECT
                                                    COUNT(*) AS num_executions
                                                FROM
                                                    dba_hist_sqlstat s
                                                    JOIN dba_hist_sql_plan p ON s.sql_id = p.sql_id AND s.plan_hash_value = p.plan_hash_value
                                                    JOIN dba_indexes i ON p.object_owner = i.owner AND p.object_name = i.index_name
                                                    JOIN dba_hist_snapshot sp ON sp.snap_id = s.snap_id
                                                WHERE
                                                    s.elapsed_time_total > 0
                                                    AND sp.BEGIN_INTERVAL_TIME >= SYSDATE - 7
                                                    AND i.index_name = :indexName
                                                GROUP BY
                                                    i.owner, i.index_name
                                            ), shared_column_indexes AS (
                                                SELECT 
                                                    b.index_name AS shared_index,
                                                    a.column_name
                                                FROM 
                                                    dba_ind_columns a
                                                    JOIN dba_ind_columns b ON a.table_name = b.table_name
                                                                        AND a.column_name = b.column_name
                                                                        AND a.index_name != b.index_name
                                                WHERE 
                                                    a.index_name = :indexName
                                                GROUP BY 
                                                    b.index_name, a.column_name
                                            )
                                            SELECT 
                                                i.blevel,
                                                i.distinct_keys,
                                                i.leaf_blocks, 
                                                i.num_rows, 
                                                i.clustering_factor,
                                                i.AVG_LEAF_BLOCKS_PER_KEY,
                                                i.AVG_DATA_BLOCKS_PER_KEY,
                                                (SELECT LISTAGG(shared_index || ' (' || column_name || ')', ', ') WITHIN GROUP (ORDER BY shared_index) FROM shared_column_indexes) AS shared_indexes,
                                                (SELECT SUM(s.bytes)/8192 FROM dba_segments s WHERE s.owner = i.table_owner AND s.segment_name = i.table_name) AS table_size_in_blocks,
                                                DECODE(i.num_rows, 0, 0, Round(i.distinct_keys/i.num_rows,4)) AS selectivity_ratio,
                                                Round((i.leaf_blocks/(SELECT SUM(s.bytes)/8192 FROM dba_segments s WHERE s.owner = i.table_owner AND s.segment_name = i.table_name)),2) as lf_tablesize_ratio,
                                                Round(i.clustering_factor/i.num_rows,2) as clust_ratio,
                                                Round((index_usage_data.num_executions / total_executions_data.total_executions) * 100,2) as index_usage_percentage
        
                                            FROM 
                                                dba_indexes i,
                                                total_executions_data,
                                                index_usage_data
                                            WHERE 
                                                i.index_name = :indexName";



            const double weightIndexUsage = 40.0;
            const double weightSelectivity = 30.0;
            const double weightLeafBlockToTableSize = 20.0;
            const double weightClusteringFactor = 10.0;

            // Define thresholds for each factor
            const double thresholdIndexUsage = 5.0;  // Less than 5% usage
            const double thresholdSelectivity = 0.29; // Less than 0.01 selectivity
            const double thresholdLeafBlockToTableSize = .20; // More than 20% size
            const double thresholdClusteringFactor = 0.70; // More than 1 clustering factor

           
            //try
            //{
                double totalScore;

                using (OracleConnection conn = new OracleConnection(Daily.connectionStrings["ASTPRD"]))
                {
                    conn.Open();
                    using (OracleCommand cmd = new OracleCommand(query, conn))
                    {
                        foreach (string indexName in indexNames)
                        {
                        cmd.Parameters.Clear(); // Clear parameters for each iteration
                        cmd.Parameters.Add("indexName", OracleDbType.Varchar2).Value = indexName;

                            using (OracleDataReader reader = cmd.ExecuteReader())
                            {
                                if (reader.Read())
                                {
                                    double blevel = Convert.ToInt16(reader["blevel"]);
                                    double distinctKeys = Convert.ToInt64(reader["distinct_keys"]);
                                    double leafBlocks = Convert.ToInt64(reader["leaf_blocks"]);
                                    double num_rows = Convert.ToInt64(reader["num_rows"]);
                                    double clusteringFactor = Convert.ToInt64(reader["clustering_factor"]);
                                    double avgleafblocksperKey = Convert.ToInt64(reader["AVG_LEAF_BLOCKS_PER_KEY"]);
                                    double avgDatablocksperKey = Convert.ToInt64(reader["AVG_DATA_BLOCKS_PER_KEY"]);
                                    double tableSizeBlocks = Convert.ToInt64(reader["table_size_in_blocks"]);
                                    double indexUsagePercentage = Convert.ToDouble(reader["index_usage_percentage"]);
                                    double selectivityRatio = Convert.ToDouble(reader["selectivity_ratio"]);
                                    double lfTableSizeRatio = Convert.ToDouble(reader["lf_tablesize_ratio"]);
                                    double clustRatio = Convert.ToDouble(reader["clust_ratio"]);
                                    string sharedIndex = Convert.ToString(reader["shared_indexes"]);
                                    Console.WriteLine($"{indexName}");
                                    Console.WriteLine();
                                    Console.WriteLine($"Library has {blevel} floors in library: BLEVEL");
                                    Console.WriteLine($"Your library has {leafBlocks} bookshelves:  Leaf Blocks");
                                    Console.WriteLine($"Your library has {distinctKeys} unique book titles: Distinct keys");
                                    Console.WriteLine($"Your library has {num_rows} books: Number of Rows");
                                    Console.WriteLine();
                                    Console.WriteLine("Clustering Ratio EXCELLENT (0 to 0.2)  Really Good (0.2 to 0.3) Good (0.4 to 0.6) Bad (0.7 to 0.9) Really Bad (0.9 to 1)");
                                    Console.WriteLine();
                                    Console.WriteLine($"Your library has a clustering factor of {clusteringFactor} and your books are {clustRatio} scattered: Clustering Ratio");
                                    Console.WriteLine();
                                    Console.WriteLine($"Total number of bookshelves you would need to spread books into a singler layer is {tableSizeBlocks}: Table Size Blocks");
                                    Console.WriteLine();
                                    Console.WriteLine("Selectivity Ratio: Good (0.7 to 0.89) Moderate (0.5 to 0.69) Fair (0.3 to 0.49) Poor (0.1 to 0.29) Very Poor (Less than 0.1)");
                                    Console.WriteLine();
                                    Console.WriteLine($"Distinct Keys (unique titles) vs Number of Rows(# books) = Selectivity: {selectivityRatio} how unique each book is within library");
                                    Console.WriteLine();
                                    Console.WriteLine("Leaf Block Table Size ratio Excellent (0.01 to 0.1) Good (0.11 to 0.3) Moderate (0.31 to 0.5) Fair (0.51 to 0.7) Poor (0.71 to 1)");
                                    Console.WriteLine();
                                    Console.WriteLine($"leaf Block vs Table Size (LF table Size ratio): {lfTableSizeRatio}");
                                    Console.WriteLine();
                                    Console.WriteLine($"Clustering vs Number of Rows: {clustRatio}");
                                    Console.WriteLine();
                                    Console.WriteLine($"Index executions vs total sql executions: {indexUsagePercentage}");

                                    // Calculate scores for each factor
                                    double scoreIndexUsage = indexUsagePercentage < thresholdIndexUsage ? weightIndexUsage : 0;
                                    double scoreSelectivity = selectivityRatio < thresholdSelectivity ? weightSelectivity : 0;
                                    double scoreLeafBlockToTableSize = lfTableSizeRatio > thresholdLeafBlockToTableSize ? weightLeafBlockToTableSize : 0;
                                    double scoreClusteringFactor = clustRatio > thresholdClusteringFactor ? weightClusteringFactor : 0;

                                    // Calculate total score
                                    totalScore = scoreIndexUsage + scoreSelectivity + scoreLeafBlockToTableSize + scoreClusteringFactor;
                                    Console.WriteLine($"Index Usage Score: {scoreIndexUsage}");
                                    Console.WriteLine($"Selectivity score: {scoreSelectivity}");
                                    Console.WriteLine($"Leaf vs Table score: {scoreLeafBlockToTableSize}");
                                    Console.WriteLine($"Clustoring score:  {scoreClusteringFactor}");
                                    Console.WriteLine();
                                    if (!string.IsNullOrEmpty(sharedIndex))
                                    {
                                        totalScore += 20;
                                        Console.WriteLine($"{indexName} shares the same column with {sharedIndex}");
                                        Console.WriteLine();
                                    }
                                    if (totalScore >= 70)
                                    {
                                        Console.WriteLine($"Total score for {indexName} is {totalScore}, considering dropping");

                                    }
                                    else
                                    {
                                        Console.WriteLine($"Total score is for {indexName} is {totalScore}, doesn't need dropping");
                                    }
                                    // Determine if index should be considered for dropping
                                    Console.ReadLine();
                                    //return totalScore >= 70;
                                    // Extract the necessary statistics from the reader

                                }
                                else
                                {
                                    Console.WriteLine($"{indexName} Needs to be dropped, index never used");
                                    Console.ReadLine();
                                    //return true;
                                }
                            }
                        }
                    }
                }
                
            //}
            //catch (Exception ex)
            //{
                // Handle or log the exception as needed
                //Console.WriteLine(ex.Message);
            //}
            Console.ReadLine();
            //Console.WriteLine("Doesn't need to be dropped");
            //return false; // Default to not dropping
        }

        public static void AnalyzeAndRebuildIndexes()
        {
            foreach (var dbName in Daily.connectionStrings.Keys)
            {
                var connectionString = Daily.connectionStrings[dbName];

                using (var conn = new OracleConnection(connectionString))
                {
                    conn.Open();
                    var indexesToRebuildFromStatusCheck = GetIndexesWithInvalidStatus(conn, dbName);

                    Parallel.ForEach(indexesToRebuildFromStatusCheck, indexName =>
                    {
                        using (var rebuildConn = new OracleConnection(connectionString))
                        {
                            rebuildConn.Open();
                            RebuildIndex(dbName, rebuildConn, indexName);
                        }
                    });

                    var indexesToCheck = GetIndexesToAnalyze(conn, dbName);

                    Parallel.ForEach(indexesToCheck, indexInfo =>
                    {
                        using (var analyzeConn = new OracleConnection(connectionString))
                        {
                            analyzeConn.Open();
                            var fullIndexName = $"{indexInfo["OWNER"]}.{indexInfo["INDEX_NAME"]}";

                            AnalyzeIndexStructure(analyzeConn, fullIndexName);

                            if (ShouldRebuildIndex(analyzeConn))
                            {
                                RebuildIndex(dbName, analyzeConn, fullIndexName);
                            }
                        }
                    });
                }
                Console.WriteLine("Press Enter to proceed to the next database...");
                Console.ReadLine();
            }
        }

        public static List<string> GetIndexesWithInvalidStatus(OracleConnection conn, string dbName)
        {
            var result = new List<string>();

            // Check for invalid status in DBA_INDEXES
            using (OracleCommand cmd = new OracleCommand("SELECT OWNER, INDEX_NAME, tablespace_name FROM DBA_INDEXES WHERE STATUS <> 'VALID' AND STATUS <> 'N/A'", conn))
            {
                using (OracleDataReader reader = cmd.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        var owner = reader.GetString(0);
                        var indexName = reader.GetString(1);
                        var tablespacename = reader.GetString(2); // you're here finish this 
                        result.Add($"{owner}.{indexName}");
                    }
                }
            }

            // Check for unusable status in DBA_IND_PARTITIONS
            using (OracleCommand cmd = new OracleCommand("SELECT INDEX_OWNER, INDEX_NAME FROM DBA_IND_PARTITIONS WHERE STATUS <> 'USABLE'", conn))
            {
                using (OracleDataReader reader = cmd.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        var owner = reader.GetString(0);
                        var indexName = reader.GetString(1);
                        result.Add($"{owner}.{indexName}");
                    }
                }
            }
            if (!result.Any())
            {
                Console.WriteLine($"All indexes are in good status in {dbName} using GetIndexesWithInvalidStatus");
            }
            return result.Distinct().ToList(); // Ensure we only have distinct index names
        }

        public static void RebuildIndex(string dbName, OracleConnection conn, string indexName)
        {
            string moveIndexCmd;
            if (dbName == "ASTPRD")
            {
                moveIndexCmd = $"ALTER INDEX {indexName} REBUILD ONLINE NOPARALLEL";
            }
            else
            {
                moveIndexCmd = $"ALTER INDEX {indexName} REBUILD NOPARALLEL";
            }

            using (OracleCommand cmd = new OracleCommand(moveIndexCmd, conn))
            {
                cmd.ExecuteNonQuery();
                if (dbName == "ASTPRD")
                {
                    Console.WriteLine($"Rebuilding ALTER INDEX {indexName} REBUILD ONLINE NOPARALLEL");
                }
                else
                {
                    Console.WriteLine($"Rebuilding ALTER INDEX {indexName} REBUILD NOPARALLEL");
                }
            }
        }

        public static List<Dictionary<string, object>> GetIndexesToAnalyze(OracleConnection conn, string dbName)
        {
            var result = new List<Dictionary<string, object>>();
            var query = @"SELECT OWNER, INDEX_NAME, CLUSTERING_FACTOR, NUM_ROWS, LEAF_BLOCKS, BLEVEL, DISTINCT_KEYS
                            FROM DBA_INDEXES
                            WHERE CLUSTERING_FACTOR IS NOT NULL
                            AND owner NOT IN ('SYS', 'SYSTEM', 'DBSNMP', 'LOFTXREF', 'DBSFWUSER', 'FLOWS_FILES', 'CTXSYS', 'ORDS_METADATA', 'LOFTREPORTS', 'AUDSYS', 'OJVMSYS', 'DVSYS', 'LOFTAUDIT', 'LOFTARCH', 'PERFSTAT', 'GSMADMIN_INTERNAL', 'APEX_220100', 'LOFTSPEC', 'GGADMIN', 'ORDDATA', 'MDSYS', 'OLAPSYS', 'LBACSYS', 'OUTLN', 'SNOMED_ADMIN', 'XDB', 'WMSYS', 'ORDSYS')
                            --AND index_name NOT LIKE 'DSCS%'
                            AND (LAST_ANALYZED IS NULL OR LAST_ANALYZED < SYSDATE - 7)
                            ORDER BY 1, 2, CLUSTERING_FACTOR DESC";

            using (OracleCommand cmd = new OracleCommand(query, conn))
            {
                using (OracleDataReader reader = cmd.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        var rowData = new Dictionary<string, object>();
                        for (int i = 0; i < reader.FieldCount; i++)
                        {
                            rowData.Add(reader.GetName(i), reader.GetValue(i));

                        }

                        result.Add(rowData);
                    }
                }
            }
            if (!result.Any())
            {
                Console.WriteLine($"All indexes are in good status in {dbName} using GetIndexesToAnalyze");
            }
            return result;
        }

        public static void AnalyzeIndexStructure(OracleConnection conn, string fullIndexName)
        {
            var sqlCommandText = $"ANALYZE INDEX {fullIndexName} VALIDATE STRUCTURE";
            using (OracleCommand cmd = new OracleCommand(sqlCommandText, conn))
            {
                try
                {
                      Console.WriteLine($"Attempting ANALYZE INDEX {fullIndexName} VALIDATE STRUCTURE");
                    cmd.ExecuteNonQuery();
                      Console.WriteLine($"Index analyzed successfully: {fullIndexName}");
                }
                catch (OracleException ex)
                {
                    if (ex.Number == 54)  // ORA-00054: resource busy
                    {
                        Console.WriteLine($"AnalyzeIndexStructure: Index {fullIndexName} could not be analyzed because it is currently in use.");
                    }
                    else
                    {
                        // Handle other Oracle errors
                        Console.WriteLine($"Oracle error {ex.Number} occurred: {ex.Message}");
                    }
                }
                catch (Exception ex)
                {
                    // Handle non-Oracle errors
                    Console.WriteLine($"An error occurred: {ex.Message}");
                }
            }
        }

        public static bool ShouldRebuildIndex(OracleConnection conn)
        {
            var query = "SELECT name, height, lf_rows, lf_blks, del_lf_rows, TO_CHAR( del_lf_rows /(DECODE(lf_rows,0,0.01,lf_rows))*100,'999.99999') ibadness FROM INDEX_STATS";
            using (OracleCommand cmd = new OracleCommand(query, conn))
            {
                using (OracleDataReader reader = cmd.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        string name = reader.GetString(0);
                        int height = reader.GetInt32(1);
                        int lf_rows = reader.GetInt32(2);
                        int del_lf_rows = reader.GetInt32(4);
                        string ibadnessString = reader.IsDBNull(5) ? "0" : reader.GetString(5);

                        // Convert the string to a decimal
                        decimal ibadness = decimal.TryParse(ibadnessString, out decimal result) ? result : 0;

                        Console.WriteLine($"Data for {name}: Height={height}, LF_Rows={lf_rows}, Deleted_LF_Rows={del_lf_rows}, Ibadness={ibadness}");

                        if (ibadness > 20M) // Assuming '20' means 20 percent (as we're multiplying by 100 in the query)
                        {
                            Console.WriteLine($"Meets criteria to rebuild {ibadness} > 20%  ShouldRebuildIndex");
                            return true;
                        }
                        else
                        {
                            Console.WriteLine($"Did not meet criteria to rebuild {ibadness} < 20% ShouldRebuildIndex");
                        }
                    }
                }
            }
            return false;
        }
        private static void EnsureIndexesInSeparateTablespaces()
        {
            foreach (var dbName in Daily.connectionStrings.Keys)
            {
                var connectionString = Daily.connectionStrings[dbName];

                using (var conn = new OracleConnection(connectionString))
                {
                    conn.Open();
                    var indexes = GetIndexesAndTablespaces(dbName, conn);

                    foreach (var index in indexes)
                    {
                        var indexOwner = index.Item1;
                        var indexName = index.Item2;
                        var indexTablespace = index.Item3;

                        if (IndexNeedsMoving(conn, indexOwner, indexName, indexTablespace))
                        {
                            //var newIndexTablespace = DetermineNewTablespace(conn, indexTablespace);
                            MoveIndexToNewTablespace(conn, indexOwner, indexName, indexTablespace, dbName);

                        }
                      
                    }
                }
                Console.WriteLine("Press Enter to proceed to the next database...");
                Console.ReadLine();
            }
        }

        private static List<Tuple<string, string, string>> GetIndexesAndTablespaces(string dbName, OracleConnection conn)
        {
            var result = new List<Tuple<string, string, string>>();
            var query = @"SELECT OWNER, INDEX_NAME, TABLESPACE_NAME 
                      FROM DBA_INDEXES 
                      where index_type='NORMAL' and owner like 'ASSURTRK%' and table_name not like '%LOB'"; // 

            using (OracleCommand cmd = new OracleCommand(query, conn))
            {
                using (OracleDataReader reader = cmd.ExecuteReader())
                    
                {
                    Console.WriteLine($"{dbName,-15}"); 
                    Console.WriteLine($"{"OWNER",-15} {"INDEX_NAME",-45} {"TABLESPACE_NAME",-15}");
                    while (reader.Read())
                    {
                        var owner = reader.GetString(0);
                        var indexName = reader.GetString(1);
                        var tablespaceName = reader.GetString(2);
                        result.Add(Tuple.Create(owner, indexName, tablespaceName));
                        // Just use -15 for left alignment without any numeric format specifier
                        Console.WriteLine($"{owner,-15} {indexName,-45} {tablespaceName,-15}");
                    }
                }
            }

            return result;
        }

        private static bool IndexNeedsMoving(OracleConnection conn, string indexOwner, string indexName, string indexTablespace)
        {
            // Query to find the tablespace of the table to which the index belongs
            var tablespaceQuery = $@"
                            SELECT DISTINCT TABLESPACE_NAME 
                            FROM DBA_TABLES 
                            WHERE OWNER = '{indexOwner}' 
                            AND TABLE_NAME = (
                                SELECT TABLE_NAME 
                                FROM DBA_INDEXES 
                                WHERE OWNER = '{indexOwner}'
                                AND INDEX_NAME = '{indexName}'
                            )";

            using (OracleCommand cmd = new OracleCommand(tablespaceQuery, conn))
            {
                // Add parameters with the names as they appear in the query
                cmd.Parameters.Add(new OracleParameter("indexOwner", OracleDbType.Varchar2)).Value = indexOwner;
                cmd.Parameters.Add(new OracleParameter("indexName", OracleDbType.Varchar2)).Value = indexName;

                var tableTablespaceObj = cmd.ExecuteScalar();
                if (tableTablespaceObj == null)
                {
                    // Handle the case when no result is returned, for example:
                    Console.WriteLine("No matching tablespace found for the given index and owner.");
                    return false;
                }

                var tableTablespace = tableTablespaceObj.ToString();
                return string.Equals(indexTablespace, tableTablespace, StringComparison.OrdinalIgnoreCase);

            }
        }

      

        private static string DetermineNewTablespace(OracleConnection conn, string currentTablespace)
        {
            // Define a list of tablespaces to exclude (for example, system tablespaces or full tablespaces)
            var excludedTablespaces = new HashSet<string> { "SYSTEM", "SYSAUX", currentTablespace };
            var query = "SELECT TABLESPACE_NAME FROM DBA_TABLESPACES";

            using (OracleCommand cmd = new OracleCommand(query, conn))
            {
                using (OracleDataReader reader = cmd.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        var tablespaceName = reader.GetString(0);
                        // Select the first tablespace that is not in the excluded list
                        if (!excludedTablespaces.Contains(tablespaceName))
                        {
                            return tablespaceName;
                        }
                    }
                }
            }

            throw new InvalidOperationException("No available tablespace found to move the index.");
        }

        private static void MoveIndexToNewTablespace(OracleConnection conn, string indexOwner, string indexName, string tablespace, string dbName)
        {
            string moveIndexCmd;
            if (dbName == "ASTPRD")
            {
                moveIndexCmd = $"ALTER INDEX {indexOwner}.{indexName} REBUILD TABLESPACE {tablespace} ONLINE";
            }
            else
            {
                moveIndexCmd = $"ALTER INDEX {indexOwner}.{indexName} REBUILD TABLESPACE {tablespace}";
            }

            using (OracleCommand cmd = new OracleCommand(moveIndexCmd, conn))
            {
                cmd.ExecuteNonQuery();
                if (dbName == "ASTPRD")
                {
                    Console.WriteLine($"Moved index {indexOwner}.{indexName} to tablespace {tablespace} ONLINE");
                }
                else
                {
                    Console.WriteLine($"Moved index {indexOwner}.{indexName} to tablespace {tablespace}");
                }
            }
        }


        


       
        public static void GatherTableStats()
        {
            foreach (var dbName in Daily.connectionStrings.Keys)
            {
                var connectionString = Daily.connectionStrings[dbName];

                using (var conn = new OracleConnection(connectionString))
                {
                    conn.Open();
                    var gatherStatsCommands = GetGatherStatsCommands(conn);

                    foreach (var tuple in gatherStatsCommands)
                    {
                        var gatherStatsCmd = tuple.Item1;
                        var tableName = tuple.Item2;
                        var owner = tuple.Item3;

                        Console.WriteLine($"{dbName}: cmd: {gatherStatsCmd} owner: {owner} table: {tableName}");
                        ExecuteGatherStats(conn, gatherStatsCmd, tableName, owner, dbName);
                    }
                }
            }
        }

        public static List<Tuple<string, string, string>> GetGatherStatsCommands(OracleConnection conn)
        {
            var result = new List<Tuple<string, string, string>>();

            var query = @"SELECT 
                                'BEGIN DBMS_STATS.GATHER_TABLE_STATS(ownname=>''' || TABLE_OWNER || ''',tabname=>''' || dtm.TABLE_NAME || ''',cascade=>true); END;' AS GATHER_STATS_CMD,
                                TABLE_OWNER, 
                                dtm.TABLE_NAME, 
                                INSERTS, 
                                UPDATES, 
                                DELETES, 
                                dt.num_rows
                            FROM DBA_TAB_MODIFICATIONS dtm
                            JOIN dba_tables dt ON dt.owner = dtm.TABLE_OWNER
                            left JOIN DBA_TAB_STATISTICS dts on dts.table_name = dtm.table_name and dts.owner = dtm.table_owner
                            WHERE table_owner like 'ASSUR%'--','ASSURTRKEMR','ASSURTRKEMR_USER','ASSURTRKHL7','ASSURTRKHL7_USER','ASSURTRKICON','ASSURTRKICON_USER','ASSURTRKOE','ASSURTRKOE_USER','ASSURTRKPAM','ASSURTRKPAM_USER','ASSURTRKPM','ASSURTRKPM_USER','ASSURTRKVOR','ASSURTRKVOR_USER','ASSURTRK_TST','ASSURTRK_USER','FAAS_ADMIN','GS_ADMIN','NDC_ADMIN','RPT_ADMIN')
                            AND (inserts+updates+deletes) > (0.10 * dt.num_rows)
                            and dtm.table_name not like ('%$%')
                            AND (dts.last_analyzed IS NULL OR trunc(dts.last_analyzed) NOT BETWEEN TRUNC(SYSDATE) - 6 AND TRUNC(SYSDATE))"; // 

            using (OracleCommand cmd = new OracleCommand(query, conn))
            {
                using (OracleDataReader reader = cmd.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        var command = reader.GetString(0);
                        var tableOwner = reader.GetString(1);
                        var tableName = reader.GetString(2);
                       ;
                        result.Add(Tuple.Create(command, tableName, tableOwner));
                    }
                }
            }

            return result;
        }

        public static void ExecuteGatherStats(OracleConnection conn, string gatherStatsCmd, string tableName, string owner, string dbName)
        {
            UnlockStats(conn, tableName, owner);
            // Execute gather stats command
            using (OracleCommand cmd = new OracleCommand(gatherStatsCmd, conn))
            {
                cmd.ExecuteNonQuery();
            }

            // Query DBA_TAB_STATISTICS
            var query = $"SELECT LAST_ANALYZED, NUM_ROWS FROM DBA_TAB_STATISTICS WHERE TABLE_NAME = '{tableName}' AND OWNER =  '{owner}'";
            using (OracleCommand cmd = new OracleCommand(query, conn))
            {
                using (OracleDataReader reader = cmd.ExecuteReader())
                {
                    if (reader.Read())
                    {
                        var lastAnalyzed = reader.GetDateTime(0);
                        var numRows = reader.GetInt64(1);
                        Console.WriteLine($"{dbName}: Table: {owner}.{tableName}, LAST_ANALYZED: {lastAnalyzed}, NUM_ROWS: {numRows}");
                    }
                }
            }
        }
        public static void UnlockStats(OracleConnection conn, string tableName, string owner)
        {
            var unlockCmd = $"BEGIN DBMS_STATS.UNLOCK_TABLE_STATS(ownname=>'{owner}',tabname=>'{tableName}'); END;";
            using (OracleCommand cmd = new OracleCommand(unlockCmd, conn))
            {
                cmd.ExecuteNonQuery();
            }
        }

        public static void LockStats(OracleConnection conn, string tableName, string owner)
        {
            var lockCmd = $"BEGIN DBMS_STATS.LOCK_TABLE_STATS(ownname=>'{owner}',tabname=>'{tableName}'); END;";
            using (OracleCommand cmd = new OracleCommand(lockCmd, conn))
            {
                cmd.ExecuteNonQuery();
            }
        }

        public static void GatherSchemaStats()
        {
            foreach (var dbName in Daily.connectionStrings.Keys)
            {
                var connectionString = Daily.connectionStrings[dbName];

                using (var conn = new OracleConnection(connectionString))
                {
                    conn.Open();
                    var schemaNames = GetSchemaNames(conn);

                    foreach (var schemaName in schemaNames)
                    {
                        var gatherSchemaStatsCmd = $"BEGIN DBMS_STATS.GATHER_SCHEMA_STATS(ownname=>'{schemaName}', options=>'GATHER AUTO', cascade=>TRUE, estimate_percent=>DBMS_STATS.AUTO_SAMPLE_SIZE); END;";
                        Console.WriteLine($"{dbName}: Gathering stats for schema: {schemaName}");
                        ExecuteGatherSchemaStats(conn, gatherSchemaStatsCmd, schemaName, dbName);
                    }
                }
            }
        }

        public static List<string> GetSchemaNames(OracleConnection conn)
        {
            var result = new List<string>();
            var query = @"SELECT DISTINCT OWNER FROM DBA_TABLES WHERE OWNER like ('ASSUR%')"; // Exclude the system schemas

            using (OracleCommand cmd = new OracleCommand(query, conn))
            {
                using (OracleDataReader reader = cmd.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        result.Add(reader.GetString(0));
                    }
                }
            }

            return result;
        }

        public static void ExecuteGatherSchemaStats(OracleConnection conn, string gatherSchemaStatsCmd, string schemaName, string dbName)
        {
            using (OracleCommand cmd = new OracleCommand(gatherSchemaStatsCmd, conn))
            {
                cmd.ExecuteNonQuery();
            }

            Console.WriteLine($"{dbName}: Schema stats gathered for {schemaName}");
        }


    }
}


