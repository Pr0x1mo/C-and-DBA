public static async Task<string> AddGS1info(string primaryDi)
{
    string url = $"https://accessgudid.nlm.nih.gov/api/v2/devices/lookup.json?di={primaryDi}";

    using (HttpClient httpClient = new HttpClient())
    {
        HttpResponseMessage response = await httpClient.GetAsync(url);

        if (response.IsSuccessStatusCode)
        {
            string content = await response.Content.ReadAsStringAsync();
            JObject json = JObject.Parse(content);
            var identifiersToken = json.SelectToken("gudid.device.identifiers");

            // Assuming 'identifiers' is always a JObject based on your JSON structure.
            var identifiersObj = (JObject)identifiersToken;

            // Iterate over each property (which represents an identifier) in the JObject
            foreach (var property in identifiersObj.Properties())
            {
                var identifier = (JObject)property.Value;
                if ((string)identifier["deviceIdIssuingAgency"] == "GS1" &&
                    (string)identifier["deviceIdentifierType"] != "Primary")
                {
                    return identifier.ToString();
                }
            }
        }
        return "{}";  // Or return null if that's preferred for indicating no GS1 found.
    }
}


private static async Task UpdateGS1Info(OracleConnection connection, string primaryDi, string GS1infoJson)
{
    JObject GS1info = JObject.Parse(GS1infoJson);

    string deviceId = (string)GS1info["deviceId"];
    string deviceIdType = (string)GS1info["deviceIdType"];
    string deviceIdIssuingAgency = (string)GS1info["deviceIdIssuingAgency"];
    string containsDINumber = (string)GS1info["containsDINumber"];
    string pkgQuantity = (string)GS1info["pkgQuantity"];  // This might be a number; if so, convert appropriately.
    DateTime? pkgDiscontinueDate = GS1info["pkgDiscontinueDate"].Type != JTokenType.Null
                               ? (DateTime?)DateTime.Parse(GS1info["pkgDiscontinueDate"].ToString())
                               : null; // This should be a date; you'll need to convert it from string to a DateTime.
    string pkgStatus = (string)GS1info["pkgStatus"];
    string pkgType = (string)GS1info["pkgType"];

    string updateQuery = @"
        UPDATE ASSURTRK.DEVICES
        SET DEVICE_ID = :deviceId,
            DEVICE_ID_TYPE = :deviceIdType,
            DEVICE_ID_ISSUING_AGENCY = :deviceIdIssuingAgency,
            CONTAINS_DI_NUMBER = :containsDINumber,
            PKG_QUANTITY = :pkgQuantity,
            PKG_DISCONTINUE_DATE = :pkgDiscontinueDate,
            PKG_STATUS = :pkgStatus,
            PKG_TYPE = :pkgType
        WHERE PRIMARYDI = :primaryDi";

    using (OracleTransaction transaction = connection.BeginTransaction())
    {
        using (OracleCommand updateCommand = new OracleCommand(updateQuery, connection))
        {
            updateCommand.Transaction = transaction;
            updateCommand.Parameters.Add(new OracleParameter("deviceId", deviceId));
            updateCommand.Parameters.Add(new OracleParameter("deviceIdType", deviceIdType));
            updateCommand.Parameters.Add(new OracleParameter("deviceIdIssuingAgency", deviceIdIssuingAgency));
            updateCommand.Parameters.Add(new OracleParameter("containsDINumber", containsDINumber));
            updateCommand.Parameters.Add(new OracleParameter("pkgQuantity", pkgQuantityNumber.HasValue ? (object)pkgQuantityNumber.Value : DBNull.Value)); // Make sure to convert to the appropriate type if needed.
            updateCommand.Parameters.Add(new OracleParameter("pkgDiscontinueDate", 
                                                 pkgDiscontinueDate.HasValue ? (object)pkgDiscontinueDate.Value : DBNull.Value)); // Make sure to convert to DateTime.
            updateCommand.Parameters.Add(new OracleParameter("pkgStatus", pkgStatus));
            updateCommand.Parameters.Add(new OracleParameter("pkgType", pkgType));
            updateCommand.Parameters.Add(new OracleParameter("primaryDi", primaryDi));

            try
            {
                int rowsAffected = await updateCommand.ExecuteNonQueryAsync();

                if (rowsAffected > 0)
                {
                    Console.WriteLine($"Updated {rowsAffected} row(s) for PRIMARYDI: {primaryDi}");
                    transaction.Commit();
                }
                else
                {
                    Console.WriteLine($"No rows updated for PRIMARYDI: {primaryDi}");
                    transaction.Rollback(); // Consider rolling back if no rows are affected
                }
            }
            catch (OracleException ex)
            {
                // Handle exceptions
                Console.WriteLine($"Error updating GS1 info: {ex.Message}");
                transaction.Rollback();
            }
        }
    }
}
