Imports System
Imports System.Collections.ObjectModel
Imports System.IO
'Imports System.Reflection.Metadata
Imports System.Threading
Imports OpenQA.Selenium
Imports OpenQA.Selenium.Chrome
Imports OpenQA.Selenium.Interactions
Imports OpenQA.Selenium.Support.UI

Module Module1
    Sub Main()


        Dim driverPath As String = "C:\Users\XavierBorja\chromedriver\chromedriver.exe"
        ' Specify the path to the Chrome profile directory (adjust as needed)
        Dim Options As New ChromeOptions()
        Options.AddArgument("start-maximized") ' Maximize the browser window
        Options.AddArgument("disable-infobars") ' Disable infobars (optional)
        Options.AddArgument("--disable-blink-features=AutomationControlled")
        Options.AddArgument("--disable-extensions")
        Options.AddArgument("--disable-popup-blocking")
        Options.AddArguments("--enable-logging=stdout")
        Options.AddArgument("--remote-debugging-port=9222") ' Specify the port for CDP
        Options.AddArgument("--auto-open-devtools-for-tabs")
        Options.AddArgument("--disable-web-security")
        Options.AddArgument("user-data-dir=C:\Users\XavierBorja\AppData\Local\Google\Chrome\User Data")

        ' Initialize the ChromeDriver with the custom options
        Dim driver As New ChromeDriver(driverPath, Options)

        Dim wait As New WebDriverWait(driver, TimeSpan.FromSeconds(10))
        Dim jsExecutor As IJavaScriptExecutor = DirectCast(driver, IJavaScriptExecutor)

        ' Navigate to the login page
        driver.Navigate().GoToUrl("https://sit.consortiex.net/vor/login")



        wait.Until(Function(d) CType(jsExecutor.ExecuteScript("return document.querySelector('#loginForm > div:nth-child(1) > div > input');"), IWebElement) IsNot Nothing)

        Dim emailField As IWebElement = driver.FindElement(By.CssSelector("#loginForm > div:nth-child(1) > div > input"))


        Dim jsCode As String = $"document.querySelector('#loginForm > div:nth-child(1) > div > input').value = 'su@test.com'"
        jsExecutor.ExecuteScript(jsCode)
        ' Fill in the email field
        'emailField.SendKeys("su@test.com")

        ' Locate the password field
        Dim passwordField As IWebElement = driver.FindElement(By.CssSelector("#loginForm > div:nth-child(2) > div > input"))
        jsCode = "document.querySelector('#loginForm > div:nth-child(2) > div > input').value = 'I41ni94E!7'"
        jsExecutor.ExecuteScript(jsCode)
        ' Fill in the password field
        'passwordField.SendKeys("I41ni94E!7")

        ' Submit the login form
        passwordField.Submit()



        ' Wait for the dropdown to become clickable
        Thread.Sleep(5000)

        ' Wait for the element with the given JavaScript query selector to be available
        wait.Until(Function(d) CType(jsExecutor.ExecuteScript("return document.querySelector('#setCaidForm > div.modal-body > div:nth-child(6)')"), IWebElement) IsNot Nothing)

        ' Execute the JavaScript code to collect dropdown options
        Dim javascriptCode As String = "
                                        var selectElement = document.querySelector('#affiliateDropdown');
                                        var optionsData = [];
                                        for (var i = 0; i < selectElement.options.length; i++) {
                                            var option = selectElement.options[i];
                                            var value = option.value;
                                            var text = option.textContent;
                                            optionsData.push({ text: text, value: value });
                                        }
                                        return optionsData;
                                    "

        ' Collect dropdown options using JavaScript
        Dim optionsData As Object = jsExecutor.ExecuteScript(javascriptCode)


        ' Handle the returned information (optionsData)
        If TypeOf optionsData Is ICollection(Of Object) Then
            Dim optionsCollection As ICollection(Of Object) = DirectCast(optionsData, ICollection(Of Object))

            ' Iterate through the collection of option information
            For Each optionInfo As Object In optionsCollection
                Console.WriteLine("Text: " & optionInfo("text").ToString() & " Value: " & optionInfo("value").ToString())
            Next
        ElseIf TypeOf optionsData Is String Then
            Console.WriteLine(optionsData.ToString())
        Else
            Console.WriteLine("Unexpected result")
        End If

        Dim selectDropdown As IWebElement = driver.FindElement(By.Id("affiliateDropdown"))

        ' Wait for a few seconds to ensure the element is available
        Thread.Sleep(5000) ' Adjust the sleep duration as needed

        ' Create a SelectElement and select the option by value
        Dim selectElement As SelectElement = New SelectElement(selectDropdown)
        selectElement.SelectByValue("2a97de19-1edb-4eb5-bbaa-53217a383bb9")

        ' Click the "Confirm" button
        Dim confirmButton As IWebElement = driver.FindElement(By.Id("setCaidButton"))
        confirmButton.Click()

        Thread.Sleep(2000)


        jsCode = "document.querySelector('#content-top-actions > div > table > tbody > tr > td:nth-child(3) > a > span.add-item-text.text-uppercase').click();"
        DirectCast(driver, IJavaScriptExecutor).ExecuteScript(jsCode)
        Thread.Sleep(2000)

        jsCode = "javascript:void((function(){andiScript=document.createElement('script');andiScript.setAttribute('src','https://www.ssa.gov/accessibility/andi/andi.js');document.body.appendChild(andiScript)})());"

        ' Execute the JavaScript code to inject ANDI into the current page
        DirectCast(driver, IJavaScriptExecutor).ExecuteScript(jsCode)

        wait.Until(Function(d) CType(jsExecutor.ExecuteScript("return document.querySelector('#ANDI508-alertGroup_0 > a')"), IWebElement) IsNot Nothing)

        Dim elementToClick As IWebElement = driver.FindElement(By.CssSelector("#ANDI508-alertGroup_0 > a"))

        ' Create an Actions object
        Dim actions As Actions = New Actions(driver)

        ' Perform a physical mouse click on the element
        actions.Click(elementToClick).Perform()

        Thread.Sleep(3000)

        ' Your JavaScript code to get the selectors
        javascriptCode = "var andiAlertsContainer = document.getElementById('ANDI508-alerts-container');
                                var selectorList = [];
                                if (andiAlertsContainer) {
                                    var alerts = andiAlertsContainer.querySelectorAll('.ANDI508-alertGroup-toggler');
                                    for (var i = -1; i < alerts.length; i++) {                                    
                                        var selector = '#ANDI508-alertGroup_0 > ol > li:nth-child(' + (i + 2) + ') > a';
                                        selectorList.push(selector);
                                    }
                                }
                                return selectorList;"

        ' Dim jsExecutor As IJavaScriptExecutor = DirectCast(driver, IJavaScriptExecutor)
        Dim selectorList As ReadOnlyCollection(Of Object) = CType(jsExecutor.ExecuteScript(javascriptCode), ReadOnlyCollection(Of Object))

        ' Convert the ReadOnlyCollection to a list of strings
        Dim stringSelectorList As New List(Of String)

        For Each obj As Object In selectorList
            stringSelectorList.Add(obj.ToString())
        Next



        Dim addOnPropertiesElements = driver.FindElements(By.CssSelector(".ANDI508-display-addOnProperties"))
        Dim ariaHasPopup As String = ""
        Dim ariaExpanded As String = ""
        Dim andi508 As String = "
            var table = document.getElementById('ANDI508-accessibleComponentsTable');
            if (table) {
                var rows = table.getElementsByTagName('tr');
                var result = [];
                for (var i = 0; i < rows.length; i++) {
                    var th = rows[i].getElementsByTagName('th')[0];
                    var td = rows[i].getElementsByTagName('td')[0];
                    if (th && td) {
                        var label = th.textContent.trim();
                        var value = td.textContent.trim();
                        result.push(label + ':: ' + value);
                    }
                }
                return result;
            }
"
        For Each selector As String In stringSelectorList
            Dim elementToClick1 As IWebElement = driver.FindElement(By.CssSelector(selector))
            actions.Click(elementToClick1).Perform()
            Thread.Sleep(3000)
            Dim elementNameContainer = driver.FindElement(By.CssSelector("#ANDI508-elementNameContainer"))
            Dim elementNameText = elementNameContainer.Text
            Console.WriteLine(elementNameText)
            Dim accessibleComponentsHeading = driver.FindElement(By.CssSelector("#ANDI508-accessibleComponentsTable-heading"))
            Dim componentsHeadingText = accessibleComponentsHeading.Text
            Console.WriteLine(componentsHeadingText)
            Dim accessibleComponentsTotal = driver.FindElement(By.CssSelector("#ANDI508-accessibleComponentsTotal"))
            Dim totalText = accessibleComponentsTotal.Text
            '  Console.WriteLine(totalText)
            Dim result As ReadOnlyCollection(Of Object) = CType(jsExecutor.ExecuteScript(andi508), ReadOnlyCollection(Of Object))


            ' Print the results
            For Each item As String In result
                Console.WriteLine(item)
            Next
            Dim andiOutput = driver.FindElement(By.CssSelector("#ANDI508-outputText .ANDI508-display-danger a"))
            Dim andiOutputText = andiOutput.Text
            Dim validFileName As String = System.Text.RegularExpressions.Regex.Replace(andiOutputText, "[^\w\d]+", "_")


            actions.MoveToElement(andiOutput).Perform()
            Console.WriteLine(andiOutputText)
            Dim screenshot = DirectCast(driver, ITakesScreenshot).GetScreenshot()
            ' Save the screenshot
            Dim screenshotFileName As String = $"{validFileName}.png"
            screenshot.SaveAsFile(screenshotFileName, ScreenshotImageFormat.Png)
            Dim folderPath As String = Path.GetDirectoryName(Path.GetFullPath(screenshotFileName))
            Process.Start("explorer.exe", folderPath)
        Next
        ' Find all elements with the class ANDI508-display-addOnProperties



        Console.WriteLine("Press Enter to exit...")
        Console.ReadLine()

        ' Close the WebDriver when done
        driver.Quit()
    End Sub




End Module
