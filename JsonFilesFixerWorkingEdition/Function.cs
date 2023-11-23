using Azure;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Threading.Tasks;

namespace JsonFilesFixerWorkingEdition
{
    public class Function
    {
        /// <summary>
        /// It builds the connection to the ENDPOINT
        /// </summary>
        /// <returns>the access Token</returns>
        public static async Task<string> GetAccessTokenAsync()
        {
            var clientId = System.Environment.GetEnvironmentVariable("CLIENT_ID", EnvironmentVariableTarget.Process);
            var clientSecret = System.Environment.GetEnvironmentVariable("CLIENT_SECRET", EnvironmentVariableTarget.Process);
            var resource = System.Environment.GetEnvironmentVariable("RESOURCE", EnvironmentVariableTarget.Process);
            var tenantId = System.Environment.GetEnvironmentVariable("TENANT_ID", EnvironmentVariableTarget.Process);

            var client = new HttpClient();

            var content = new FormUrlEncodedContent(new[]
            {
                new KeyValuePair<string, string>("grant_type", "client_credentials"),
                new KeyValuePair<string, string>("client_id", clientId),
                new KeyValuePair<string, string>("client_secret", clientSecret),
                new KeyValuePair<string, string>("resource", resource)
            });

            var response = await client.PostAsync($"https://login.microsoftonline.com/{tenantId}/oauth2/token", content);
            var responseString = await response.Content.ReadAsStringAsync();
            var jsonString = JsonConvert.DeserializeObject<GetResource>(responseString);

            //Console.WriteLine(jsonString.access_token);

            return jsonString.access_token;
        }

        [FunctionName("TheChosenFunctionv3")]
        public async Task Run([BlobTrigger("insights-logs-signinlogs/{name}.json", Connection = "AzureWebJobsStorage")] BlobBaseClient blob, ILogger log)
        {
            await DoTheMagic(blob, blob.Name, log);
            //log.LogError("Counter is now outside: " + count);
        }
        /*
        [FunctionName("TheBrotherOfTheChosenOnev2")]
        public async Task RunZero([BlobTrigger("json-test-try/{name}.json", Connection = "AzureWebJobsStorage")] BlobBaseClient blob,
        string name, ILogger log)
        {
            await DoTheMagic(blob, name, log);
        }
        */
        /*
        [FunctionName("Function2")]
        public async Task RunOne([BlobTrigger("new-try/{name}.json", Connection = "AzureWebJobsStorage")] BlobBaseClient blob,
        string name,
        ILogger log)
        {
            await DoTheMagic(blob, name, log);
        }

        [FunctionName("Function3")]
        public async Task RunTwo([BlobTrigger("new-try/{name}.json", Connection = "AzureWebJobsStorage")] BlobBaseClient blob,
        string name,
        ILogger log)
        {
            await DoTheMagic(blob, name, log);
        }

        [FunctionName("Function4")]
        public async Task RunThree([BlobTrigger("new-try/{name}.json", Connection = "AzureWebJobsStorage")] BlobBaseClient blob,
        string name,
        ILogger log)
        {
            await DoTheMagic(blob, name, log);
        }

        [FunctionName("Function5")]
        public async Task RunFour([BlobTrigger("new-try/{name}.json", Connection = "AzureWebJobsStorage")] BlobBaseClient blob,
        string name,
        ILogger log)
        {
            await DoTheMagic(blob, name, log);
        }

        [FunctionName("Function6")]
        public async Task RunFive([BlobTrigger("new-try/{name}.json", Connection = "AzureWebJobsStorage")] BlobBaseClient blob,
        string name,
        ILogger log)
        {
            await DoTheMagic(blob, name, log);
        }

        [FunctionName("Function7")]
        public async Task RunSeven([BlobTrigger("new-try/{name}.json", Connection = "AzureWebJobsStorage")] BlobBaseClient blob,
        string name,
        ILogger log)
        {
            await DoTheMagic(blob, name, log);
        }

        [FunctionName("Function8")]
        public async Task RunEight([BlobTrigger("new-try/{name}.json", Connection = "AzureWebJobsStorage")] BlobBaseClient blob,
        string name,
        ILogger log)
        {
            await DoTheMagic(blob, name, log);
        }

        [FunctionName("Function9")]
        public async Task RunNine([BlobTrigger("new-try/{name}.json", Connection = "AzureWebJobsStorage")] BlobBaseClient blob,
        string name,
        ILogger log)
        {
            await DoTheMagic(blob, name, log);
        }
        */
        public static async Task DoTheMagic(BlobBaseClient blob, string name, ILogger log)
        {
            var connectionString = System.Environment.GetEnvironmentVariable("AzureWebJobsStorage", EnvironmentVariableTarget.Process);

            //string containerName = name.Replace($"/PT1H", "");
            string containerName = blob.BlobContainerName;
            string fileName = name;
            //log.LogWarning($"Full Path of JSON file: {containerName}/{fileName}");
            log.LogWarning($"Actual Full Path of JSON file: {name}");
            string month = DateTime.Now.Month.ToString();
            string day = DateTime.Now.Day.ToString();
            int hour = DateTime.Now.Hour - 2;
            if (int.Parse(month) < 10) { month = "0" + month; }
            if (int.Parse(day) < 10) { day = "0" + day; }
            //log.LogWarning($"Algotithmic Full Path of JSON file: y={DateTime.Now.Year}/m={month}/d={day}/h={hour}");
            //log.LogWarning($"Name: {name}");
            //log.LogWarning($"blob.Name: {blob.Name}");
            bool boolOne = name.Contains($"y={DateTime.Now.Year}/m={month}/d={day}/h={hour}");
            bool boolTwo = IsAppendBlobOpenForAppend(connectionString, containerName, fileName, log);
            log.LogWarning(boolOne.ToString());
            log.LogWarning(boolTwo.ToString());
            if (name.Contains($"y={DateTime.Now.Year}/m={month}/d={day}/h={hour}") && IsAppendBlobOpenForAppend(connectionString, containerName, fileName, log))
            /*if (name.Contains($"y={DateTime.Now.Year}/m={month}/d={day}/h={DateTime.Now.Hour}"))*/
            {
                log.LogError($"The container {name} will be skipped");
                await Task.Delay(TimeSpan.FromMinutes(0).Add(TimeSpan.FromSeconds(59)));
                await DoTheMagic(blob, name, log);
            }
            log.LogError("It's only a test");
            return;

            log.LogWarning($"C# Blob trigger function processed blob\n Name:{name} \n Uri:{blob.Uri}");

            // Generates a new name for the updated blob
            string newJsonName = $"{name}_updated_json_{DateTime.UtcNow.ToString("yyyyMMddTHHmmssfff")}.json";
            string exceptionLogFileName = $"ExceptionLogFile_updated_{DateTime.UtcNow.ToString("yyyyMMddTHHmmssfff")}.txt";

            // Create RetryOptions and set the NetworkTimeout
            var options = new BlobClientOptions
            {
                Retry =
            {
                MaxRetries = 10,
                NetworkTimeout = TimeSpan.FromMinutes(30)
            }
            };
            //var connectionString = System.Environment.GetEnvironmentVariable("AzureWebJobsStorage", EnvironmentVariableTarget.Process);
            BlobContainerClient container = new BlobContainerClient(connectionString, blob.BlobContainerName, options);


            // Check if the updated blob already exists
            if (blob.Name.Contains("updated"))
            {
                if (!(blob.GetType() == typeof(AppendBlobClient)))
                {
                    log.LogError($"Updated blob '{name}' already exists.");
                    return;
                }
                log.LogError($"Updated blob '{name}' already exists.");
                return;
            }

            log.LogInformation($"Processing new blob '{name}'.");

            // Reads the contents of the blob
            using (Stream stream = await blob.OpenReadAsync())
            using (StreamReader reader = new StreamReader(stream))
            {
                var jsonObjects = new List<object>();

                // Adds the content of JSON to the List of jsonObjects
                try
                {
                    string line;
                    while ((line = reader.ReadLine()) != null)
                    {
                        var jsonObject = JsonConvert.DeserializeObject(line);
                        jsonObjects.Add(jsonObject);
                    }
                }

                catch (Exception ex)
                {
                    // Sends the exception message to the ENDPOINT
                    var accessToken = await GetAccessTokenAsync();
                    var client = new HttpClient();
                    client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", accessToken);

                    //PostAsJsonAsync - with string jsonData
                    string responseUrl = System.Environment.GetEnvironmentVariable("CONNECTION_STRING_LogError_JsonRepairer", EnvironmentVariableTarget.Process);
                    string functionUrl = System.Environment.GetEnvironmentVariable("JSONRepairFunction_URL", EnvironmentVariableTarget.Process);
                    string jsonData = "{\"applikation\": " + System.Reflection.Assembly.GetExecutingAssembly().GetName().Name.ToString() + ", \"methode\": " + System.Reflection.MethodBase.GetCurrentMethod().Name.ToString() + ", \"fehlermeldung\": " + ex.Message + ", \"stacktrace\": " + ex.StackTrace + ", \"link\": " + functionUrl + "}";

                    ErrorBody deptObj = JsonConvert.DeserializeObject<ErrorBody>(jsonData);

                    var response = await client.PostAsJsonAsync(responseUrl, deptObj);

                    var responseString = await response.Content.ReadAsStringAsync();
                    log.LogInformation(responseString);
                }

                var json = JsonConvert.SerializeObject(jsonObjects, Formatting.Indented);

                // Converts the updated contents of the JSON file to a byte array
                byte[] newJsonContentsBytes = Encoding.UTF8.GetBytes(json);

                // Uploads the updated JSON blob with a new name
                BlobClient updatedJsonBlob = container.GetBlobClient(newJsonName);

                await updatedJsonBlob.UploadAsync(new MemoryStream(newJsonContentsBytes), overwrite: false);
            }

            // Deletes the old blob
            //await blob.DeleteAsync();

            log.LogInformation($"Blob '{name}' updated. New blob name: '{newJsonName}'.");
        }

        public static bool IsAppendBlobOpenForAppend(string connectionString, string containerName, string blobName, ILogger log)
        {
            //BlobServiceClient blobServiceClient = new BlobServiceClient(connectionString);
            //BlobContainerClient containerClient = blobServiceClient.GetBlobContainerClient(containerName);
            //BlobClient blobClient = containerClient.GetBlobClient(blobName);
            BlobClient blobClient = new(connectionString, containerName, blobName);

            /*
            // Set the Content-Type to octet-stream
            BlobHttpHeaders httpHeaders = new BlobHttpHeaders
            {
                ContentType = "application/octet-stream"
            };
            */

            //log.LogWarning(httpHeaders.ContentType);
            // Set the Content-Type for the blob
            //blobClient.SetHttpHeaders(httpHeaders);

            //log.LogWarning(httpHeaders.ContentType);

            // Get the blob properties
            BlobProperties blobProperties = blobClient.GetProperties();

            /*
            Response<BlobProperties> response = blobClient.GetProperties();
            log.LogWarning(response.Value.ContentType);
            // Check if the Content-Type is "application/octet-stream"
            if (response.Value.ContentType == "application/octet-stream")
            {
                // Update the Content-Type to "application/json"
                blobClient.SetHttpHeaders(new BlobHttpHeaders { ContentType = "application/json" });
            }
            //return !response.Value.IsSealed;
            */

            return !blobProperties.IsSealed;
        }
    }

    public class GetResource
    {
        public string token_type { get; set; }
        public string expires_in { get; set; }
        public string ext_expires_in { get; set; }
        public string expires_on { get; set; }
        public string not_before { get; set; }
        public string resource { get; set; }
        public string access_token { get; set; }
    }

    public class ErrorBody
    {
        public string applikation { get; set; }
        public string methode { get; set; }
        public string fehlermeldung { get; set; }
        public string stacktrace { get; set; }
        public string link { get; set; }

        public ErrorBody()
        {

        }
        public ErrorBody(string applikation, string methode, string fehlermeldung, string stacktrace, string link)
        {
            this.applikation = applikation;
            this.methode = methode;
            this.fehlermeldung = fehlermeldung;
            this.stacktrace = stacktrace;
            this.link = link;
        }
    }

}