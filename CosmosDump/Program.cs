using System;
using System.IO;
using System.Threading.Tasks;
using CommandLine;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Microsoft.Azure.Documents.Linq;

namespace CosmosDump
{
    static class Program
    {
        // ReSharper disable once ClassNeverInstantiated.Local
        // ReSharper disable UnusedAutoPropertyAccessor.Local
        private class Options
        {
            [Option('e', "endpoint-uri", Required = true, HelpText = "URI for the CosmosDB Endpoint (not connection string)")]
            public string EndpointUri { get; set; }
            [Option('k', "api-key", Required = true, HelpText = "API Key")]
            public string ApiKey { get; set; }
            [Option('d', "database", Required = true, HelpText = "Database name")]
            public string Database { get; set; }
            [Option('c', "collection", Required = true, HelpText = "Collection name")]
            public string Collection { get; set; }
            [Option('o', "output", Required = false, Default = null, HelpText =  "Output directory (default = current directory)")]
            public string Output { get; set; }
        }
        // ReSharper restore UnusedAutoPropertyAccessor.Local
        
        // ReSharper disable once ArrangeTypeMemberModifiers
        static void Main(string[] args)
        {
            Parser.Default.ParseArguments<Options>(args)
                .WithParsed(o =>
                {
                    var client = new DocumentClient(new Uri(o.EndpointUri), o.ApiKey);
                    Dump(client, o.Database, o.Collection, o.Output ?? Directory.GetCurrentDirectory()).Wait(); 
                });
        }

        private static async Task Dump(IDocumentClient client, string database, string collection, string outputDirectory)
        {
            var query = client.CreateDocumentQuery(
                UriFactory.CreateDocumentCollectionUri(database, collection),
                new FeedOptions {EnableCrossPartitionQuery = true, MaxBufferedItemCount = 10}
            );

            var queryAll = query.AsDocumentQuery();
            var count = await query.CountAsync();

            var filename = Path.Combine(outputDirectory, $"{collection}.json");

            if (File.Exists(filename))
            {
                Console.WriteLine($"Overwriting file {filename}");
            }
            Console.WriteLine($"Writing {count} items from {collection} to {filename}...");

            using (var stream = File.CreateText(filename))
            {   
                var items = 0;
                while (queryAll.HasMoreResults)
                {
                    var docs = await queryAll.ExecuteNextAsync();

                    foreach (var document in docs)
                    {
                        await stream.WriteLineAsync(document.ToString());
                        items++;
                    }
                    
                    Console.Write($"{items}...");
                }
                Console.WriteLine();
                Console.WriteLine($"Finished exporting {collection}");
            }
        }
    }
}
