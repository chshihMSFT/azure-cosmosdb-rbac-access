using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.Threading;
using System.Linq;
using Azure.Identity;
using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Cosmos.Linq;
using Microsoft.Extensions.Configuration;
using System.Diagnostics;

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

    Cosmos DB Built-in Data Reader: 00000000-0000-0000-0000-000000000001
    Microsoft.DocumentDB/databaseAccounts/readMetadata
    Microsoft.DocumentDB/databaseAccounts/sqlDatabases/containers/items/read
    Microsoft.DocumentDB/databaseAccounts/sqlDatabases/containers/executeQuery
    Microsoft.DocumentDB/databaseAccounts/sqlDatabases/containers/readChangeFeed
 
    Cosmos DB Built-in Data Contributor: 00000000-0000-0000-0000-000000000002
    Microsoft.DocumentDB/databaseAccounts/readMetadata
    Microsoft.DocumentDB/databaseAccounts/sqlDatabases/containers/*
    Microsoft.DocumentDB/databaseAccounts/sqlDatabases/containers/items/*

    Authentication demo in this sample:
    KeyAuth
    RBAC_AADauth
    RBAC_MIauth (default)
*/

namespace azure_cosmosdb_rbac_access
{
    class Program
    {
        static String application_name;
        static String aad_tenant_id;
        static String aad_managed_user_id;
        static String aad_application_id;
        static String aad_application_secret;
        static String RBACTestMode;
        static String cosmosdb_uri;
        static String cosmosdb_accountkey;
        static String cosmosdb_dbname;
        static String cosmosdb_containername;
        static String cosmosdb_datamodel;
        static int request_interval;

        static async Task Main(string[] args)
        {
            Console.WriteLine($"{DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.ffffff")}, Azure Cosmos DB - RBAC access demo ... start");

            IConfigurationRoot configuration = new ConfigurationBuilder()
                        .AddJsonFile("appSettings.json")
                        .Build();
            application_name = configuration["application_name"].ToString();
            aad_tenant_id = configuration["aad_tenant_id"].ToString();
            aad_managed_user_id = configuration["aad_managed_user_id"].ToString();
            aad_application_id = configuration["aad_application_id"].ToString();
            aad_application_secret = configuration["aad_application_secret"].ToString();
            RBACTestMode = configuration["RBACTestMode"].ToString();
            cosmosdb_uri = configuration["cosmosdb_uri"].ToString();
            cosmosdb_accountkey = configuration["cosmosdb_accountkey"].ToString();
            cosmosdb_dbname = configuration["cosmosdb_dbname"].ToString();
            cosmosdb_containername = configuration["cosmosdb_containername"].ToString();
            request_interval = Int32.Parse(configuration["request_interval"]);
            cosmosdb_datamodel = configuration["cosmosdb_datamodel"].ToString();

            CosmosClientOptions clientOptions = new CosmosClientOptions()
            {
                AllowBulkExecution = true
                , ConnectionMode = ConnectionMode.Direct
                , ApplicationName = String.Format($"{RBACTestMode}_{application_name}")
                , MaxRetryAttemptsOnRateLimitedRequests = 300
                , MaxRetryWaitTimeOnRateLimitedRequests = TimeSpan.FromSeconds(15)
                , CosmosClientTelemetryOptions = new CosmosClientTelemetryOptions { DisableSendingMetricsToService = false }
            };

            CosmosClient keyClient;            
            CosmosClient RBACclientAAD;
            CosmosClient RBACclientMI;
            Database database;
            Container container;
            switch (RBACTestMode)
            {
                case "KeyAuth":
                    keyClient = new CosmosClient(cosmosdb_uri, cosmosdb_accountkey, clientOptions);

                    database = keyClient.GetDatabase(cosmosdb_dbname);
                    container = keyClient.GetContainer(cosmosdb_dbname, cosmosdb_containername);
                    break;

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

                default:
                case "RBAC_MIauth":
                    //RBAC-based authentication #2 - Managed Identity
                    var tokenCredential = new DefaultAzureCredential(
                        new DefaultAzureCredentialOptions
                        {
                            TenantId = aad_tenant_id
                        });

                    RBACclientMI = new CosmosClient(cosmosdb_uri, tokenCredential, clientOptions);
                    database = RBACclientMI.GetDatabase(cosmosdb_dbname);
                    container = RBACclientMI.GetContainer(cosmosdb_dbname, cosmosdb_containername);
                    break;
            }

            String fmt = "0000.00";
            if (cosmosdb_datamodel == "DocumentDB")
            {
                //Data-Plane CRUD test
                try
                {
                    Stopwatch stopwatch = new Stopwatch();
                    Item demodoc = new Item();
                    int i = 1;
                    while (true) 
                    {
                        demodoc.id = Guid.NewGuid().ToString();
                        demodoc.pk = RBACTestMode;
                        demodoc.counter = i;
                        demodoc.timestamp = DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.ffffff");

                        //1. Create Item
                        ItemResponse<Item> responseCreate = null;
                        try
                        {
                            stopwatch.Restart();
                            responseCreate = await container.CreateItemAsync<Item>(demodoc, new PartitionKey(demodoc.pk));
                        }
                        catch (CosmosException ex)
                        {
                            Console.WriteLine($"{DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.ffffff")}, round {i.ToString("D8")}, CreateItem,\t\tFailed: {ex.Message}" +
                            $"Diagnostics: {ex}");
                        }
                        finally
                        {
                            stopwatch.Stop();
                            Item pointCreateResult = responseCreate.Resource;
                            Console.WriteLine($"{DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.ffffff")}, round {i.ToString("D8")}, CreateItem,\t\tConsumed:{responseCreate.RequestCharge.ToString(fmt)} RUs in {stopwatch.Elapsed.TotalMilliseconds.ToString(fmt)} ms,\t" +
                                $"ActivityId:{responseCreate.ActivityId}, " +
                                $"{{id: {pointCreateResult.id}, pk: {pointCreateResult.pk}, counter: {pointCreateResult.counter}, _ts: {pointCreateResult._ts}, _etag: {pointCreateResult._etag}}}");
                        }
                        Thread.Sleep(request_interval);

                        //2. Upsert Item
                        demodoc.counter = -1;
                        ItemResponse<Item> responseUpsert = null;                        
                        try
                        {
                            stopwatch.Restart();
                            responseUpsert = await container.UpsertItemAsync<Item>(demodoc, new PartitionKey(demodoc.pk));
                        }
                        catch (CosmosException ex)
                        {
                            Console.WriteLine($"{DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.ffffff")}, round {i.ToString("D8")}, UpsertItem,\t\tFailed: {ex.Message}" +
                            $"Diagnostics: {ex}");
                        }
                        finally
                        {
                            stopwatch.Stop();
                            Item pointUpsertResult = responseUpsert.Resource;
                            Console.WriteLine($"{DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.ffffff")}, round {i.ToString("D8")}, UpsertItem,\t\tConsumed:{responseUpsert.RequestCharge.ToString(fmt)} RUs in {stopwatch.Elapsed.TotalMilliseconds.ToString(fmt)} ms,\t" +
                                $"ActivityId:{responseUpsert.ActivityId}, " +
                                $"{{id: {pointUpsertResult.id}, pk: {pointUpsertResult.pk}, counter: {pointUpsertResult.counter}, _ts: {pointUpsertResult._ts}, _etag: {pointUpsertResult._etag}}}");
                        }
                        Thread.Sleep(request_interval);

                        //3.1 Read Item
                        ItemResponse<Item> responseRead = null;
                        try
                        {
                            stopwatch.Restart();
                            responseRead = await container.ReadItemAsync<Item>(demodoc.id, new PartitionKey(demodoc.pk));                            
                        }
                        catch (CosmosException ex)
                        {
                            Console.WriteLine($"{DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.ffffff")}, round {i.ToString("D8")}, ReadItem,\t\tFailed: {ex.Message}" +
                            $"Diagnostics: {ex}");
                        }
                        finally
                        {
                            stopwatch.Stop();
                            Item pointReadResult = responseRead.Resource;
                            Console.WriteLine($"{DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.ffffff")}, round {i.ToString("D8")}, ReadItem,\t\tConsumed:{responseRead.RequestCharge.ToString(fmt)} RUs in {stopwatch.Elapsed.TotalMilliseconds.ToString(fmt)} ms,\t" +
                                $"ActivityId:{responseRead.ActivityId}, " +
                                $"{{id: {pointReadResult.id}, pk: {pointReadResult.pk}, counter: {pointReadResult.counter}, _ts: {pointReadResult._ts}, _etag: {pointReadResult._etag}}}");
                        }
                        Thread.Sleep(request_interval);

                        //3.2 Read Many Item
                        // Create item list with (id, pkvalue) tuples
                        List<(string, PartitionKey)> itemReadList = new List<(string, PartitionKey)>
                        {
                            ("demodoc001", new PartitionKey("demo")),
                            ("demodoc001", new PartitionKey(demodoc.pk)),
                            (demodoc.id, new PartitionKey(demodoc.pk))                      
                        };
                        
                        FeedResponse<Item> responseReadMany = null;
                        try
                        {
                            stopwatch.Restart();
                            responseReadMany = await container.ReadManyItemsAsync<Item>(itemReadList);
                        }
                        catch (CosmosException ex)
                        {
                            Console.WriteLine($"{DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.ffffff")}, round {i.ToString("D8")}, ReadManyItem,\tFailed: {ex.Message}" +
                            $"Diagnostics: {ex}");
                        }
                        finally
                        {
                            stopwatch.Stop();
                            Console.WriteLine($"{DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.ffffff")}, round {i.ToString("D8")}, ReadManyItem,\tConsumed:{responseReadMany.RequestCharge.ToString(fmt)} RUs in {stopwatch.Elapsed.TotalMilliseconds.ToString(fmt)} ms,\t" +
                                $"ActivityId:{responseReadMany.ActivityId}.");
                            foreach (Item pointReadManyResult in responseReadMany)
                            {
                                Console.WriteLine($"{{id: {pointReadManyResult.id}, pk: {pointReadManyResult.pk}, counter: {pointReadManyResult.counter}, _ts: {pointReadManyResult._ts}, _etag: {pointReadManyResult._etag}}}");
                            }
                        }
                        Thread.Sleep(request_interval);
                        
                        //4.1 Query Item - LINQ
                        var queryOption = new QueryRequestOptions
                        {
                            ConsistencyLevel = ConsistencyLevel.Session,
                            MaxItemCount = -1
                        };
                        List<Item> docs = new List<Item>();

                        try
                        {
                            stopwatch.Restart();
                            using (FeedIterator<Item> itemIterator = container.GetItemLinqQueryable<Item>(requestOptions: queryOption)
                                    .Where(b => b.id == demodoc.id)
                                    .ToFeedIterator<Item>()
                                )
                            {
                                stopwatch.Stop();
                                while (itemIterator.HasMoreResults)
                                {
                                    var queryReadResult = await itemIterator.ReadNextAsync();
                                    foreach (Item doc in queryReadResult)
                                    {
                                        Console.WriteLine($"{DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.ffffff")}, round {i.ToString("D8")}, QueryItemLINQ,\tConsumed:{queryReadResult.RequestCharge.ToString(fmt)} RUs in {stopwatch.Elapsed.TotalMilliseconds.ToString(fmt)} ms,\t" +
                                            $"ActivityId:{responseRead.ActivityId}, " +
                                            $"{{id: {doc.id}, pk: {doc.pk}, counter: {doc.counter}, _ts: {doc._ts}, _etag: {doc._etag}}}");
                                    }
                                }
                            }

                        }
                        catch (CosmosException ex)
                        {
                            Console.WriteLine($"{DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.ffffff")}, round {i.ToString("D8")}, QueryItemLINQ,\tFailed: {ex.Message}" +
                            $"Diagnostics: {ex}");
                        }
                        Thread.Sleep(request_interval);

                        //4.2 QueryItems - QueryText
                        QueryDefinition query = new QueryDefinition(
                            $"SELECT * FROM c where c.pk = '{demodoc.pk}' and c.id in ('demodoc001','{demodoc.id}')"
                            );
                        try {
                            using (FeedIterator<Item> queryResultSet = container.GetItemQueryIterator<Item>(
                                query,
                                requestOptions: new QueryRequestOptions()
                                {
                                    ConsistencyLevel = ConsistencyLevel.Session,
                                    MaxItemCount = -1
                                    //, ResponseContinuationTokenLimitInKb = 1
                                    //, PopulateIndexMetrics = true //https://docs.microsoft.com/en-us/azure/cosmos-db/sql/index-metrics                    
                                }
                            ))
                            {
                                Console.WriteLine($"{DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.ffffff")}, round {i.ToString("D8")}, QueryItemTEXT,\t\"{query.QueryText}\"");
                                while (queryResultSet.HasMoreResults)
                                {
                                    foreach (Item doc in await queryResultSet.ReadNextAsync())
                                    {   
                                        Console.WriteLine($"{{id: {doc.id}, pk: {doc.pk}, counter: {doc.counter}, _ts: {doc._ts}, _etag: {doc._etag}}}");
                                    }
                                }
                            }
                        }
                        catch (CosmosException ex)
                        {
                            Console.WriteLine($"{DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.ffffff")}, round {i.ToString("D8")}, QueryItemTEXT,\tFailed: {ex.Message}" +
                            $"Diagnostics: {ex}");
                        }
                        Thread.Sleep(request_interval);

                        //5. Delete Item
                        ItemResponse<Item> responseDelete = null;
                        try
                        {
                            stopwatch.Restart();
                            responseDelete = await container.DeleteItemAsync<Item>(demodoc.id, new PartitionKey(demodoc.pk));
                        }
                        catch (CosmosException ex)
                        {
                            Console.WriteLine($"{DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.ffffff")}, round {i.ToString("D8")}, DeleteItem,\t\tFailed: {ex.Message}" +
                            $"Diagnostics: {ex}");
                        }
                        finally
                        {
                            stopwatch.Stop();
                            Item pointReadResult = responseRead.Resource;
                            Console.WriteLine($"{DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.ffffff")}, round {i.ToString("D8")}, DeleteItem,\t\tConsumed:{responseDelete.RequestCharge.ToString(fmt)} RUs in {stopwatch.Elapsed.TotalMilliseconds.ToString(fmt)} ms,\t" +
                                $"ActivityId:{responseDelete.ActivityId}, " +
                                $"{{id: {demodoc.id}, pk: {demodoc.pk}");
                        }
                        Thread.Sleep(request_interval);

                        i++;
                    }
                }
                catch (CosmosException ce)
                {
                    Console.WriteLine($"{DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.ffffff")}, RBAC Item testing ... Error: {ce.Message}");
                }
            }

            if (cosmosdb_datamodel == "Graph")
            {
                //Data-Plane CRUD test
                try
                {
                    GraphVertex demoVertex = new GraphVertex();
                    GraphEdge demoEdge = new GraphEdge();
                    int i = 1;
                    while (true)
                    {
                        demoVertex.label = Guid.NewGuid().ToString();
                        demoVertex.id = Guid.NewGuid().ToString();
                        demoVertex.pk = RBACTestMode;
                        demoVertex.counter = new List<GraphVertexProp> { new GraphVertexProp {
                            id = Guid.NewGuid().ToString(),
                            _value = i.ToString()
                        } };
                        demoVertex.timestamp = new List<GraphVertexProp> { new GraphVertexProp {
                            id = Guid.NewGuid().ToString(),
                            _value = DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.ffffff")
                        } };

                        demoVertex.property01 = new List<GraphVertexProp> { new GraphVertexProp {
                            id = Guid.NewGuid().ToString(),
                            _value = "test"
                        } };

                        //1.1 Create Vertex
                        ItemResponse<GraphVertex> responseVCreate = await container.CreateItemAsync<GraphVertex>(demoVertex, new PartitionKey(demoVertex.pk));
                        GraphVertex pointCreateVResult = responseVCreate.Resource;
                        Console.WriteLine($"{DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.ffffff")}, round {i.ToString("D8")}, CreateVertex, Consumed:{responseVCreate.RequestCharge.ToString(fmt)} RUs, ActivityId:{responseVCreate.ActivityId}" +
                            $", {{id: {pointCreateVResult.id}, pk: {pointCreateVResult.pk}, counter: {pointCreateVResult.counter}, _ts: {pointCreateVResult._ts}, _etag: {pointCreateVResult._etag}}}");

                        ////1.2 Create Vertex-Edge
                        for (int j = 1; j <= 500000; j++)
                        {
                            demoEdge.label = String.Format($"EdgeLabelTest{j.ToString("D8")}");
                            demoEdge.id = Guid.NewGuid().ToString();
                            demoEdge.pk = RBACTestMode;
                            demoEdge._isEdge = true;
                            demoEdge._sink = "6c11e0a7-fb54-4202-9e2b-efebb6b0d257";
                            demoEdge._sinkLabel = String.Format($"SinkLabel{j.ToString("D8")}");
                            demoEdge._sinkPartition = RBACTestMode;
                            demoEdge._vertexId = "4937eabb-9439-4324-b64e-be91cfb69f4e";
                            demoEdge._vertexLabel = String.Format($"VertexLabel{j.ToString("D8")}");

                            ItemResponse<GraphEdge> responseECreate = await container.CreateItemAsync<GraphEdge>(demoEdge, new PartitionKey(demoEdge.pk));
                            GraphEdge pointCreateEResult = responseECreate.Resource;
                            Console.WriteLine($"{DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.ffffff")}, round {j.ToString("D8")}, CreateEdges, Consumed:{responseECreate.RequestCharge.ToString(fmt)} RUs, ActivityId:{responseECreate.ActivityId}" +
                            $", {{id: {pointCreateEResult.id}, pk: {pointCreateEResult.pk}, _ts: {pointCreateEResult._ts}, _etag: {pointCreateEResult._etag}}}");

                            Thread.Sleep(100);
                        }
                        Thread.Sleep(request_interval);

                        i++;
                    }
                }
                catch (CosmosException ce)
                {
                    Console.WriteLine($"{DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.ffffff")}, RBAC Item testing ... Error: {ce.Message}");
                }

            }

        }

    }
}
