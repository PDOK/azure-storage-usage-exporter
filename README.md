# Azure storage usage exporter

[![Build](https://github.com/PDOK/azure-storage-usage-exporter/actions/workflows/build-and-publish-image.yml/badge.svg)](https://github.com/PDOK/azure-storage-usage-exporter/actions/workflows/build-and-publish-image.yml)
[![Lint (go)](https://github.com/PDOK/azure-storage-usage-exporter/actions/workflows/lint-go.yml/badge.svg)](https://github.com/PDOK/azure-storage-usage-exporter/actions/workflows/lint-go.yml)
[![Go Report Card](https://goreportcard.com/badge/github.com/PDOK/azure-storage-usage-exporter)](https://goreportcard.com/report/github.com/PDOK/azure-storage-usage-exporter)
[![Coverage (go)](https://github.com/PDOK/azure-storage-usage-exporter/wiki/coverage.svg)](https://raw.githack.com/wiki/PDOK/azure-storage-usage-exporter/coverage.html)
[![GitHub license](https://img.shields.io/github/license/PDOK/azure-storage-usage-exporter)](https://github.com/PDOK/azure-storage-usage-exporter/blob/master/LICENSE)
[![Docker Pulls](https://img.shields.io/docker/pulls/pdok/azure-storage-usage-exporter.svg)](https://hub.docker.com/r/pdok/azure-storage-usage-exporter)

This app generates and exports/exposes statistics about cloud storage usage.
It is initially tailored to PDOK's use case:

* The cloud storage is [Azure Blob storage](https://azure.microsoft.com/en-us/products/storage/blobs). 
* Usage concerns (current) occupation, not transactions.
* The aggregation groups are: container, owner and dataset.

It does so by:

* Periodically aggregating an [Azure Storage blob inventory report](https://learn.microsoft.com/en-us/azure/storage/blobs/blob-inventory).
* Applying rules about which container/directory/prefix applies to which dataset and owner combo.
* Exposing the results in a prometheus endpoint.

## Build

```shell
docker build .
```

## Run

```text
USAGE:
   azure-storage-usage-exporter [global options] command [command options] 

COMMANDS:
   help, h  Shows a list of commands or help for one command

GLOBAL OPTIONS:
   --azure-storage-connection-string value  Connection string for connecting to the Azure blob storage that holds the inventory [$AZURE_STORAGE_CONNECTION_STRING]
   --bind-address value                     The TCP network address addr that is listened on. (default: ":8080") [$BIND_ADDRESS]
   --blob-inventory-container value         Name of the container that holds the inventory (default: "blob-inventory") [$BLOB_INVENTORY_CONTAINER]
   --extra-rules-file value                 File to read extra rules from (they will come before the default rules) [$EXTRA_RULES_FILE]
   --help, -h                               show help
```

### Extra rules file

Example of extra rules file contents:

```yaml
- pattern: ^(?P<container>special)/(?P<owner>[^/]+)/.+)
  dataset: my-dataset # constant arbitrary _dataset_ label overrides dataset group from regex (could be left out of pattern)
- pattern: ^(?P<container>[^/]+)/(?P<owner>[^/]+)/(?P<dataset>[^/]+)
```

### Observability

#### Health checks

Health endpoint is available on `/health`.

### Linting

Install [golangci-lint](https://golangci-lint.run/usage/install/) and run `golangci-lint run`
from the root.
