using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using CommandLine;
using Microsoft.Azure.CosmosDB.BulkExecutor;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace CosmosRestore
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
            [Option('i', "input-file", Required = false, Default = null, HelpText =  "Input file containing JSON documents (1 per line)")]
            public string InputFile { get; set; }
        }
        // ReSharper restore UnusedAutoPropertyAccessor.Local
        
        // ReSharper disable once ArrangeTypeMemberModifiers
        static void Main(string[] args)
        {
            Parser.Default.ParseArguments<Options>(args)
                .WithParsed(o =>
                {
                    var client = new DocumentClient(
                        new Uri(o.EndpointUri),
                        o.ApiKey,
                        new ConnectionPolicy
                        {
                            ConnectionMode = ConnectionMode.Direct,
                            ConnectionProtocol = Protocol.Tcp
                        });
                    Restore(client, o.Database, o.Collection, o.InputFile).Wait(); 
                });
        }

        private static async Task Restore(DocumentClient client, string database, string collection, string inputFile)
        {
            // ReSharper disable once ReplaceWithSingleCallToFirst
            var documentCollection = client
                .CreateDocumentCollectionQuery(UriFactory.CreateDatabaseUri(database))
                .Where(c => c.Id == collection)
                .AsEnumerable()
                .First();
            
            client.ConnectionPolicy.RetryOptions.MaxRetryWaitTimeInSeconds = 30;
            client.ConnectionPolicy.RetryOptions.MaxRetryAttemptsOnThrottledRequests = 9;
            
            var bulk = new BulkExecutor(client, documentCollection);
            await bulk.InitializeAsync();
            
            client.ConnectionPolicy.RetryOptions.MaxRetryWaitTimeInSeconds = 0;
            client.ConnectionPolicy.RetryOptions.MaxRetryAttemptsOnThrottledRequests = 0;
            
            var filename = Path.Combine(Directory.GetCurrentDirectory(), inputFile);
            
            if (!File.Exists(filename))
            {
                Console.WriteLine($"File {filename} not found.");
            }
            
            using (var stream = File.OpenRead(filename)) Console.WriteLine($"Attempting to import {CountLinesMaybe(stream)} records from {filename}.");

            var records = new List<object>(10);
            var complete = 0;
            using (var stream = File.OpenRead(filename))
            using (var reader = new StreamReader(stream))
            {
                while (!reader.EndOfStream)
                {
                    for (var i = 0; i < 10 && !reader.EndOfStream; i++)
                    {
                        var line = await reader.ReadLineAsync();
                        var document = JsonConvert.DeserializeObject(line);
                        records.Add(document);
                    }
                    
                    await bulk.BulkImportAsync(records, enableUpsert: true);
                    complete += records.Count;
                    Console.Write($"{complete}...");
                    records.Clear();
                }
            }
            Console.WriteLine("Done!");

        }

        private const char CR = '\r';  
        private const char LF = '\n';  
        private const char NULL = (char)0;


        public static long CountLinesMaybe(Stream stream)  
        {
            var lineCount = 0L;

            var byteBuffer = new byte[1024 * 1024];
            const int BytesAtTheTime = 4;
            var detectedEOL = NULL;
            var currentChar = NULL;

            int bytesRead;
            while ((bytesRead = stream.Read(byteBuffer, 0, byteBuffer.Length)) > 0)
            {
                var i = 0;
                for (; i <= bytesRead - BytesAtTheTime; i += BytesAtTheTime)
                {
                    currentChar = (char)byteBuffer[i];

                    if (detectedEOL != NULL)
                    {
                        if (currentChar == detectedEOL) { lineCount++; }

                        currentChar = (char)byteBuffer[i + 1];
                        if (currentChar == detectedEOL) { lineCount++; }

                        currentChar = (char)byteBuffer[i + 2];
                        if (currentChar == detectedEOL) { lineCount++; }

                        currentChar = (char)byteBuffer[i + 3];
                        if (currentChar == detectedEOL) { lineCount++; }
                    }
                    else
                    {
                        if (currentChar == LF || currentChar == CR)
                        {
                            detectedEOL = currentChar;
                            lineCount++;
                        }
                        i -= BytesAtTheTime - 1;
                    }
                }

                for (; i < bytesRead; i++)
                {
                    currentChar = (char)byteBuffer[i];

                    if (detectedEOL != NULL)
                    {
                        if (currentChar == detectedEOL) { lineCount++; }
                    }
                    else
                    {
                        if (currentChar == LF || currentChar == CR)
                        {
                            detectedEOL = currentChar;
                            lineCount++;
                        }
                    }
                }
            }

            if (currentChar != LF && currentChar != CR && currentChar != NULL)
            {
                lineCount++;
            }
            return lineCount;
        }
    }
}
