using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Office.Interop.Excel;
using Oracle.DataAccess.Client;


namespace OracleDBATasks
{
    internal class ASH_Analytics
    {

        public static string excelFilePath = Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.Desktop), "ASH STAT CHART.xlsx");
       

        public static void ASH_STAT_REPORT()
        {

           
            string connectionString = "Data Source=(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)(HOST=10.1.1.213)(PORT=1521)))(CONNECT_DATA=(SERVER=DEDICATED)(SERVICE_NAME=astprd_pdb1.prodprisubphx.consortiexpxvcn.oraclevcn.com)));User Id=xborja;Password=Seraphim24$";
            
            string query = @"select TO_CHAR(TRUNC(sample_time, 'HH'), 'MM/DD/YYYY') AS date_time,                    
                   to_char(trunc((sample_time),'HH'),'HH24:MI') as time, 
                   state, 
                   CAST(count(*)/360 AS BINARY_DOUBLE) AS normalized_count
                from
                  (select sample_time, sample_id       
                  ,  CASE WHEN session_state = 'ON CPU' THEN 'CPU'       
                         WHEN session_state = 'WAITING' AND wait_class IN ('User I/O') THEN 'IO'
                         WHEN session_state = 'WAITING' AND wait_class IN ('Cluster') THEN 'CLUSTER'
                         ELSE 'WAIT' END state            
                    from DBA_HIST_ACTIVE_SESS_HISTORY             
                    where session_type IN ( 'FOREGROUND') 
                    and sample_time BETWEEN SYSDATE - 1 AND SYSDATE
                    )
                group by trunc((sample_time),'HH'), state order by trunc((sample_time),'HH')";

            using (var conn = new OracleConnection(connectionString))
            {
                conn.Open();

                using (var cmd = new OracleCommand(query, conn))
                using (var adapter = new OracleDataAdapter(cmd))
                {
                    var dataTable = new System.Data.DataTable();
                    adapter.Fill(dataTable);

                    UpdateExcelSheet(dataTable);
                }
            }
        }

        private static void UpdateExcelSheet(System.Data.DataTable dataTable)
        {
            EnsureExcelIsClosed();
            Application oXL = new Application();
            Workbook mWorkBook = oXL.Workbooks.Open(excelFilePath);
            Sheets mWorkSheets = mWorkBook.Worksheets;

            Worksheet worksheet = (Worksheet)mWorkSheets.Item["ASH Stat Chart"];

            int lastUsedRow = worksheet.Cells.SpecialCells(XlCellType.xlCellTypeLastCell, Type.Missing).Row;
            if (lastUsedRow > 1)
            {
                Range rangeToClear = worksheet.Range["A2", worksheet.Cells[lastUsedRow, dataTable.Columns.Count]];
                rangeToClear.ClearContents(); // Clear existing contents from second row onwards
            }

            for (int i = 0; i < dataTable.Rows.Count; i++)
            {
                for (int j = 0; j < dataTable.Columns.Count; j++)
                {
                    worksheet.Cells[i + 2, j + 1] = dataTable.Rows[i][j]; // Start from row 2
                }
            }

            // RefreshPivotTables(mWorkBook); // Uncomment if needed
            mWorkBook.Close(SaveChanges: true);
            oXL.Quit();
            Process.Start(excelFilePath); // Open the file after processing
        }


        private static void RefreshPivotTables(Workbook mWorkBook)
        {
            try
            {
                Worksheet pivotSheet = mWorkBook.Sheets["ASHpivot"] as Worksheet; // Replace with the actual sheet name
                if (pivotSheet != null)
                {
                    PivotTable pivotTable = pivotSheet.PivotTables("ashPivotTable") as PivotTable; // Replace with the actual PivotTable name
                    if (pivotTable != null)
                    {
                        pivotTable.RefreshTable();
                        Console.WriteLine("PivotTable refreshed successfully.");
                    }
                    else
                    {
                        Console.WriteLine("PivotTable not found.");
                    }
                }
                else
                {
                    Console.WriteLine("Worksheet not found.");
                }
            }
            catch (COMException ex)
            {
                Console.WriteLine("Error refreshing PivotTable: " + ex.Message);
                // Handle the exception as needed
            }
        }

        private static void EnsureExcelIsClosed()
        {
            foreach (var process in Process.GetProcessesByName("EXCEL"))
            {
                try
                {
                    process.Kill();
                    process.WaitForExit(); // Wait for the process to exit
                }
                catch (Exception ex)
                {
                    Console.WriteLine("Could not close Excel process: " + ex.Message);
                    // Handle exceptions if needed
                }
            }
        }

    }
}