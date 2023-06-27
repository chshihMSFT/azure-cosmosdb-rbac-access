using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.Threading;
using System.Linq;
using Azure.Identity;
using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Cosmos.Linq;
using Microsoft.Extensions.Configuration;

/*
Prerequisites:
    1. assign your application with proper RBAC permission, please check doc (https://learn.microsoft.com/en-us/azure/cosmos-db/how-to-setup-rbac)
    2. or use the following Azure CLI sample to grant built-in role permission to your application:
        $subscriptionid = "your_subscription_id"
        $resourceGroupName = "your_resource_group_name"
        $accountName = "your_cosmosdb_account_name"
        az account set --subscription $subscriptionid
 
        $buildInRoleId = "00000000-0000-0000-0000-000000000002" #Cosmos DB Built-in Data Contributor
        $principalId = "your_application_object_id" #AAD Application ObjectId, not Application Id
        az cosmosdb sql role assignment create --resource-group $resourceGroupName --account-name $accountName --scope "/" --principal-id $principalId --role-definition-id $buildInRoleId
*/

namespace azure_cosmosdb_rbac_access
{
    class Program
    {
        static String aad_tenant_id;
        static String aad_application_id;
        static String aad_application_secret;
        static String RBACTestMode;
        static String cosmosdb_uri;
        static String cosmosdb_accountkey;
        static String cosmosdb_dbname;
        static String cosmosdb_containername;
        static async Task Main(string[] args)
        {
            Console.WriteLine($"{DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.ffffff")}, Azure Cosmos DB - RBAC access demo ... start");

            IConfigurationRoot configuration = new ConfigurationBuilder()
                        .AddJsonFile("appSettings.json")
                        .Build();
            aad_tenant_id = configuration["aad_tenant_id"].ToString();
            aad_application_id = configuration["aad_application_id"].ToString();
            aad_application_secret = configuration["aad_application_secret"].ToString();
            RBACTestMode = configuration["RBACTestMode"].ToString();
            cosmosdb_uri = configuration["cosmosdb_uri"].ToString();
            cosmosdb_accountkey = configuration["cosmosdb_accountkey"].ToString();
            cosmosdb_dbname = configuration["cosmosdb_dbname"].ToString();
            cosmosdb_containername = configuration["cosmosdb_containername"].ToString();

            CosmosClientOptions clientOptions = new CosmosClientOptions()
            {
                ConnectionMode = ConnectionMode.Direct
                , ApplicationName = String.Format($"test_with_{RBACTestMode}")
                , MaxRetryAttemptsOnRateLimitedRequests = 300
                , MaxRetryWaitTimeOnRateLimitedRequests = TimeSpan.FromSeconds(15)
            };

            CosmosClient keyClient;            
            CosmosClient RBACclientAAD;
            CosmosClient RBACclientMI;
            Database database;
            Container container;
            switch (RBACTestMode)
            {
                default:
                case "RBAC_AADauth":
                    //RBAC-based authentication #1 - AAD App
                    ClientSecretCredential servicePrincipal = new ClientSecretCredential(
                        aad_tenant_id,
                        aad_application_id,
                        aad_application_secret);
                    RBACclientAAD = new CosmosClient(cosmosdb_uri, servicePrincipal, clientOptions);
                    database = RBACclientAAD.GetDatabase(cosmosdb_dbname);
                    container = RBACclientAAD.GetContainer(cosmosdb_dbname, cosmosdb_containername);
                    break;

                case "RBAC_MIauth":
                    //RBAC-based authentication #2 - Managed Identity
                    var tokenCredential = new DefaultAzureCredential();
                    //https://learn.microsoft.com/en-us/dotnet/api/azure.identity.defaultazurecredential?view=azure-dotnet
                    //var tokenCredential = new DefaultAzureCredential(new DefaultAzureCredentialOptions { ManagedIdentityClientId = aad_application_id });
                    RBACclientMI = new CosmosClient(cosmosdb_uri, tokenCredential, clientOptions);
                    database = RBACclientMI.GetDatabase(cosmosdb_dbname);
                    container = RBACclientMI.GetContainer(cosmosdb_dbname, cosmosdb_containername);
                    break;

                case "KeyAuth":
                    //Key-based authentication
                    keyClient = new CosmosClient(cosmosdb_uri, cosmosdb_accountkey, clientOptions);
                    database = await keyClient.CreateDatabaseIfNotExistsAsync(cosmosdb_dbname);
                    container = keyClient.GetContainer(cosmosdb_dbname, cosmosdb_containername);
                    break;
            }

            String fmt = "0000.00";
            int sleepTimer = 1000;//1000 = 1 sec
            
            //Data-Plane CRUD test
            try
            {
                Item demodoc = new Item();
                int i = 1;
                while (true)
                {
                    demodoc.id = Guid.NewGuid().ToString();
                    demodoc.pk = RBACTestMode;
                    demodoc.counter = i;
                    demodoc.timestamp = DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.ffffff");

                    //1. Create Item
                    ItemResponse<Item> responseCreate = await container.CreateItemAsync<Item>(demodoc, new PartitionKey(demodoc.pk));
                    Item pointCreateResult = responseCreate.Resource;

                    Console.WriteLine($"{DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.ffffff")}, round {i.ToString("D8")}, CreateItem, Consumed:{responseCreate.RequestCharge.ToString(fmt)} RUs, ActivityId:{responseCreate.ActivityId}" +
                        $", {{id: {pointCreateResult.id}, pk: {pointCreateResult.pk}, counter: {pointCreateResult.counter}, _ts: {pointCreateResult._ts}, _etag: {pointCreateResult._etag}}}");
                    Thread.Sleep(sleepTimer);

                    //2. Upsert Item
                    demodoc.counter = -1;
                    ItemResponse<Item> responseUpsert = await container.UpsertItemAsync<Item>(demodoc, new PartitionKey(demodoc.pk));
                    Item pointUpsertResult = responseUpsert.Resource;

                    Console.WriteLine($"{DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.ffffff")}, round {i.ToString("D8")}, UpsertItem, Consumed:{responseUpsert.RequestCharge.ToString(fmt)} RUs, ActivityId:{responseUpsert.ActivityId}" +
                        $", {{id: {pointUpsertResult.id}, pk: {pointUpsertResult.pk}, counter: {pointUpsertResult.counter}, _ts: {pointUpsertResult._ts}, _etag: {pointUpsertResult._etag}}}");
                    Thread.Sleep(sleepTimer);

                    //3. Read Item
                    ItemResponse<Item> responseRead = await container.ReadItemAsync<Item>(demodoc.id, new PartitionKey(demodoc.pk));
                    Item pointReadResult = responseRead.Resource;

                    Console.WriteLine($"{DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.ffffff")}, round {i.ToString("D8")}, ReadItem,   Consumed:{responseRead.RequestCharge.ToString(fmt)} RUs, ActivityId:{responseRead.ActivityId}" +
                        $", {{id: {pointReadResult.id}, pk: {pointReadResult.pk}, counter: {pointReadResult.counter}, _ts: {pointReadResult._ts}, _etag: {pointReadResult._etag}}}");
                    Thread.Sleep(sleepTimer);

                    //4. Query Item
                    var queryOption = new QueryRequestOptions
                    {
                        ConsistencyLevel = ConsistencyLevel.Session,
                        MaxItemCount = -1
                    };
                    List<Item> docs = new List<Item>();
                    using (FeedIterator<Item> itemIterator = container.GetItemLinqQueryable<Item>(requestOptions: queryOption)
                            .Where(b => b.id == demodoc.id)
                            .ToFeedIterator<Item>()
                        )
                    {
                        while (itemIterator.HasMoreResults)
                        {
                            var queryReadResult = await itemIterator.ReadNextAsync();
                            foreach (Item doc in queryReadResult)
                            {
                                Console.WriteLine($"{DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.ffffff")}, round {i.ToString("D8")}, QueryItem,  Consumed:{queryReadResult.RequestCharge.ToString(fmt)} RUs, ActivityId:{queryReadResult.ActivityId}" +
                                    $", {{id: {doc.id}, pk: {doc.pk}, counter: {doc.counter}, _ts: {doc._ts}, _etag: {doc._etag}}}");
                            }
                        }
                    }
                    Thread.Sleep(sleepTimer);

                    //5. Delete Item
                    ItemResponse<Item> responseDelete = await container.DeleteItemAsync<Item>(demodoc.id, new PartitionKey(demodoc.pk));

                    Console.WriteLine($"{DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.ffffff")}, round {i.ToString("D8")}, DeleteItem, Consumed:{responseDelete.RequestCharge.ToString(fmt)} RUs, ActivityId:{responseDelete.ActivityId}" +
                        $", {{id: {demodoc.id}, pk: {demodoc.pk}}}");
                    Thread.Sleep(sleepTimer);

                    i++;
                }
            }
            catch (Exception ce)
            {
                Console.WriteLine($"{DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.ffffff")}, RBAC Item testing ... Error: {ce.Message}");
            }

        }
    }
}
