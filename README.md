# CosmosTools
Tools for use with Azure Cosmos DB

## Getting started
1. Clone the repo
1. `dotnet build`

## Usage

### CosmosDump

This is a bit like mongodump... at its simplest we just want to export all the documents in a database to a file.

    $ ./CosmosDump -e https://mycosmosdb.documents.azure.com:443/ `
      -k mysuperlongencodedkey `
      -d MyDatabase `
      -c MyCollection

You'll get a file named `MyCollection.json` in your current directory.

For a full list of options run `./CosmosDump -h`.
