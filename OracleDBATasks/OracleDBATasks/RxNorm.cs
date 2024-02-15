using System;
using System.IO;
using System.Diagnostics;
using System.Threading;
using OpenQA.Selenium;
using OpenQA.Selenium.Chrome;
using WebDriverManager;
using WebDriverManager.DriverConfigs.Impl;
using OpenQA.Selenium.Support.UI;
using Renci.SshNet;
using System.Text.RegularExpressions;
using System.Text;

namespace OracleDBATasks
{
    public class RxNorm
    {
        public static void WednesdayETL()
        {
            // Check if today is a Wednesday and get the current or next Wednesday's date
            DateTime currentDate = DateTime.Now;
            DateTime targetWednesdayDate;

            if (currentDate.DayOfWeek == DayOfWeek.Wednesday)
            {
                // Today is Wednesday, so use the current date
                targetWednesdayDate = currentDate;
            }
            else
            {
                // Find the next Wednesday
                int daysUntilPreviousWednesday = ((int)currentDate.DayOfWeek - (int)DayOfWeek.Wednesday + 7) % 7;
                targetWednesdayDate = currentDate.AddDays(-daysUntilPreviousWednesday);
            }

            // Format the date as "MMddyyyy" (e.g., 09132023)
            string formattedDate = targetWednesdayDate.ToString("MMddyyyy");

            string downloadDirectory = @"C:\Users\XavierBorja\Downloads\";

            ChromeOptions options = new ChromeOptions();
            options.AddArgument("start-maximized");
            options.AddArgument("disable-infobars");
            options.AddArgument("--disable-blink-features=AutomationControlled");
            options.AddArgument("--disable-extensions");
            options.AddArgument("--disable-popup-blocking");
            options.AddArguments("--enable-logging=stdout");
            options.AddArgument("--remote-debugging-port=9222");
            options.AddArgument("--auto-open-devtools-for-tabs");
            options.AddArgument("--disable-web-security");
            options.AddUserProfilePreference("download.default_directory", downloadDirectory);

            ChromeDriver driver = new ChromeDriver(options);
            WebDriverWait wait = new WebDriverWait(driver, TimeSpan.FromSeconds(10));
            driver.Manage().Timeouts().ImplicitWait = TimeSpan.FromSeconds(3600);
            driver.Navigate().GoToUrl($"https://uts.nlm.nih.gov/uts/login?service=https:%2F%2Fdownload.nlm.nih.gov%2Fumls%2Fkss%2Frxnorm%2FRxNorm_weekly_{formattedDate}.zip");
            //driver.Navigate().GoToUrl("https://accounts.google.com/ServiceLogin");

            IJavaScriptExecutor jsExecutor = (IJavaScriptExecutor)driver;
            jsExecutor.ExecuteScript("document.querySelector('#google_login').click();");
            IWebElement usernameField = driver.FindElement(By.Id("identifierId"));
            usernameField.SendKeys("consortiexumls@gmail.com");

            IWebElement nextButton = driver.FindElement(By.Id("identifierNext"));
            nextButton.Click();

            wait.Until(d => true);
            IWebElement passwordInput = driver.FindElement(By.CssSelector("#password > div.aCsJod.oJeWuf > div > div.Xb9hP > input"));

            // Enter the password
            string password = "Milwaukee!23";
            passwordInput.SendKeys(password);

            // Find and click the "Next" button after entering the password
            IWebElement signInButton = driver.FindElement(By.Id("passwordNext"));
            signInButton.Click();
            wait.Until(d => true);
            
            jsExecutor.ExecuteScript("console.log('I am going to URL');");
            wait.Until(d => true);
           
            jsExecutor.ExecuteScript("console.log('I am testing');");
            wait.Until(d => true);
           
            string zipFileName = $"RxNorm_weekly_{formattedDate}.zip";
            bool fileExists = false;
            int timeoutInSeconds = 120;
            Stopwatch stopwatch = new Stopwatch();
            stopwatch.Start();

            while (true)
            {
                // Check if the ZIP file exists in the download directory
                if (File.Exists(Path.Combine(downloadDirectory, zipFileName)))
                {
                    fileExists = true;
                    break;
                }

                // Sleep for a short duration before checking again
                Thread.Sleep(1000);

                // Optionally, you can add a timeout condition to prevent an infinite loop
                if (stopwatch.Elapsed.TotalSeconds > timeoutInSeconds)
                {
                    break;
                }
            }

            stopwatch.Stop();

            // Close the browser after the download is complete
            //driver.Quit();

            //Call ExecuteWinSCPScript after download completion
            if (fileExists)
            {
                Console.WriteLine($"Zipped file exists: {zipFileName}");
                ExecuteWinSCPScript(formattedDate);
            }
            else
            {
                Console.WriteLine("Error: Download did not complete within the specified timeout.");
            }
        }

        public static void ExecuteWinSCPScript(string wednesday)
        {
            string winscpPath = @"C:\Program Files (x86)\WinSCP\winscp.com";
            string winscpScript = $"open sftp://opc@10.0.2.14:22/ -privatekey=C:\\Users\\XavierBorja\\Documents\\putty\\opc.ppk\n" +
                                  "option confirm off\n" +
                                  $"put \"C:\\Users\\XavierBorja\\Downloads\\RxNorm_weekly_{wednesday}.zip\" /tmp/\n" +
                                  "exit\n";

            var psi = new ProcessStartInfo
            {
                FileName = winscpPath,
                UseShellExecute = false,
                RedirectStandardInput = true,
                CreateNoWindow = true
            };

            using (var process = new Process { StartInfo = psi })
            {
                process.Start();

                using (var sw = process.StandardInput)
                {
                    if (sw.BaseStream.CanWrite)
                    {
                        sw.WriteLine(winscpScript);
                    }
                }

                process.WaitForExit();
            }

            Console.WriteLine("File transferred to Linux successfully.");

            string privateKeyFilePath = @"C:\Users\XavierBorja\Documents\putty\opc_ssh";
            var privateKeyFile = new PrivateKeyFile(privateKeyFilePath);
            var commandsToExecute = new[]
            {
                "chmod 777 /tmp/RxNorm_weekly_*",
                "sudo su - oracle",
                "cd scripts/rxnorm",
                "cp /tmp/RxNorm_weekly_* /share/oracle/rxnorm",
                "./extract_rxnorm_data.sh",
                "exit",
                "rm /tmp/RxNorm_weekly_*"
            };

            using (var client = new SshClient("10.0.2.14", 22, "opc", privateKeyFile))
            {
                try
                {
                    client.Connect();
                    if (client.IsConnected)
                    {
                        Console.WriteLine("Connected to SIT SSH server.");

                        using (var shellStream = client.CreateShellStream("xterm", 80, 24, 800, 600, 1024))
                        {
                            WaitForCommandCompletion(shellStream);
                            foreach (var command in commandsToExecute)
                            {
                                SendCommand(shellStream, command);
                                WaitForCommandCompletion(shellStream);
                                ReadOutput(shellStream);
                            }
                        }

                        client.Disconnect();
                    }
                    else
                    {
                        Console.WriteLine("SSH connection failed for SIT.");
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Error for SIT: {ex.Message}");
                }
            }
        }

        private static void SendCommand(ShellStream shellStream, string command)
        {
            var writer = new StreamWriter(shellStream) { AutoFlush = true };
            writer.WriteLine(command);
        }

        private static void WaitForCommandCompletion(ShellStream shellStream)
        {
            // Adjust the delay as needed, or implement a more sophisticated completion check.
            Thread.Sleep(1000);
        }

        private static void ReadOutput(ShellStream shellStream)
        {
            var reader = new StreamReader(shellStream, Encoding.UTF8);
            var output = reader.ReadToEnd();

            // Remove control codes (escape sequences)
            output = Regex.Replace(output, "←]0;oracle@[^ ]+ ", "");
            Console.WriteLine(output);
        }
    }
}
