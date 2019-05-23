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

### CosmosRestore

This is a bit like mongorestore... now we just want to get documents we've exported to a file back into a database.

    $ ./CosmosRestore -e https://mycosmosdb.documents.azure.com:443/ `
      -k mysuperlongencodedkey `
      -d MyDatabase `
      -c MyCollection `
      -i ./MyCollection.json

Documents are expected to be 1 per line and in JSON format (basically, you want to restore a dump). CosmosRestore will bulk import 10 at a time in upsert mode.

For a full list of options run `./CosmosRestore -h`.
