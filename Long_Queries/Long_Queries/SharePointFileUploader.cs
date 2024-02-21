using System.IO;
using System.Security;
using System;
using System.Net;
using Microsoft.SharePoint.Client;
using System.Diagnostics;

namespace Long_Queries
{
    internal class SharePointFileUploader
    {
        private string excelFilePath;
        private string targetFolder;

        public SharePointFileUploader(string excelFilePath, string targetFolder)
        {
            this.excelFilePath = excelFilePath;
            this.targetFolder = targetFolder;
            UploadFile(excelFilePath, targetFolder);
        }

        public void UploadFile(string excelFilePath, string targetFolder)
        {
            string siteUrl = "https://consortiex-my.sharepoint.com/personal/xavier_borja_consortiex_com/";
            string userName = "Xavier.Borja@consortiex.com";
            string password = "nzjdlklyngcmhwvm"; // Please secure this sensitive information

            try
            {
                SecureString securePassword = new SecureString();
                foreach (char c in password)
                {
                    securePassword.AppendChar(c);
                }

                var onlineCredentials = new SharePointOnlineCredentials(userName, securePassword);

                using (var clientContext = new ClientContext(siteUrl))
                {
                    clientContext.Credentials = onlineCredentials;
                    var web = clientContext.Web;
                    var newFile = new FileCreationInformation();
                    byte[] fileContent = System.IO.File.ReadAllBytes(excelFilePath);
                    newFile.ContentStream = new MemoryStream(fileContent);
                    newFile.Url = Path.GetFileName(excelFilePath);
                    string targetFolderUrl = $"{siteUrl.TrimEnd('/')}/{targetFolder.TrimStart('/')}";
                    var uploadFolder = web.GetFolderByServerRelativeUrl(targetFolderUrl);
                    var existingFile = uploadFolder.Files.GetByUrl(newFile.Url);
                    if (existingFile != null)
                    {
                        existingFile.DeleteObject(); // Delete the existing file
                    }

                    var uploadFile = uploadFolder.Files.Add(newFile);
                    clientContext.Load(uploadFile);
                    clientContext.ExecuteQuery();

                    // Set the sharing settings
                    // uploadFile.ListItemAllFields["SharedWithUsersId"] = null;
                    uploadFile.ListItemAllFields["SharedWithUsers"] = "1";
                    uploadFile.ListItemAllFields.Update();
                    clientContext.ExecuteQuery();

                    Console.ForegroundColor = ConsoleColor.Green;
                    Console.WriteLine("The File has been uploaded and made shareable" + Environment.NewLine + "FileUrl -->" + $"{targetFolderUrl}{Path.GetFileName(excelFilePath)}");
                    Process.Start(targetFolderUrl);
                }
               
            }
            catch (Exception exp)
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine(exp.Message + Environment.NewLine + exp.StackTrace);
            }
            finally
            {
                Console.ReadLine();
            }
        }

    }
}