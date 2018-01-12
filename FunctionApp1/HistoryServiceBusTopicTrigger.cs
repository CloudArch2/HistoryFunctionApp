using Microsoft.Azure.WebJobs;

using Microsoft.Azure.WebJobs.Host;

using Microsoft.ServiceBus.Messaging;

using Microsoft.Azure;
using Microsoft.WindowsAzure;

using Microsoft.WindowsAzure.Storage;

using Microsoft.WindowsAzure.Storage.Table;

using Newtonsoft.Json.Linq;

using System.IO;

using System.Runtime.Serialization.Json;

using System.Text;

using System.Runtime.Serialization;

using Newtonsoft.Json;

using System.Collections.Generic;

/*
 using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.ServiceBus.Messaging;

namespace HistoryFunctionApp
{
    public static class HistoryServiceBusTopicTrigger
    {
        [FunctionName("HistoryServiceBusTopicTrigger")]
        public static void Run([ServiceBusTrigger("cloud2fidotopic", "History", AccessRights.Manage, Connection = "FidoServBusConnectStr")]string mySbMsg, TraceWriter log)
        {
            log.Info($"C# ServiceBus topic trigger function processed message: {mySbMsg}");
        }
    }
}
*/



namespace HistoryFunctionApp

{

    public static class HistoryServiceBusTopicTrigger

    {

        [FunctionName("HistoryServiceBusTopicTrigger")]

        public static void Run([ServiceBusTrigger("cloud2fidotopic", "History", AccessRights.Manage, Connection = "FidoServBusConnectStr")]string mySbMsg, TraceWriter log)
       // public static void Run([ServiceBusTrigger("fidoscattersearch", "mysubscription", AccessRights.Manage, Connection = "conn")]string msg, TraceWriter log)

        {

            log.Info($"C# ServiceBus topic trigger function processed message: {mySbMsg}");

            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(CloudConfigurationManager.GetSetting("cloud2historystr"));


            //log.Info("i am here====================================");
            // Create the table client.

            CloudTableClient tableClient = storageAccount.CreateCloudTableClient();



            // Retrieve a reference to the table.

            CloudTable table = tableClient.GetTableReference("REQUESTS");

            log.Info("NAME-> " + table.Name);

            // Deserialization from JSON  





            RequestEntity obj = JsonConvert.DeserializeObject<RequestEntity>(mySbMsg);

            log.Info("ParitionKey->" + obj.PartitionKey);

            obj.json = mySbMsg;



            TableOperation insertOperation = TableOperation.Insert(obj);



            // Execute the insert operation.

            table.Execute(insertOperation);



        }

    }

}







public class RequestEntity : TableEntity

{

    private string cid;

    private string sid;

    private string payload;



    public string correlationId

    {

        get { return cid; }

        set
        {

            cid = value;

            this.RowKey = value;

        }

    }

    public string storeId

    {

        get { return sid; }

        set
        {

            sid = value;

            this.PartitionKey = value;

        }

    }



    public string json

    {

        get { return payload; }

        set { payload = value; }

    }

}
