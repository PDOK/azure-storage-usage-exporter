# Azure storage usage exporter

[![Build](https://github.com/PDOK/azure-storage-usage-exporter/actions/workflows/build-and-publish-image.yml/badge.svg)](https://github.com/PDOK/azure-storage-usage-exporter/actions/workflows/build-and-publish-image.yml)
[![Lint (go)](https://github.com/PDOK/azure-storage-usage-exporter/actions/workflows/lint-go.yml/badge.svg)](https://github.com/PDOK/azure-storage-usage-exporter/actions/workflows/lint-go.yml)
[![Go Report Card](https://goreportcard.com/badge/github.com/PDOK/azure-storage-usage-exporter)](https://goreportcard.com/report/github.com/PDOK/azure-storage-usage-exporter)
[![Coverage (go)](https://github.com/PDOK/azure-storage-usage-exporter/wiki/coverage.svg)](https://raw.githack.com/wiki/PDOK/azure-storage-usage-exporter/coverage.html)
[![GitHub license](https://img.shields.io/github/license/PDOK/azure-storage-usage-exporter)](https://github.com/PDOK/azure-storage-usage-exporter/blob/master/LICENSE)
[![Docker Pulls](https://img.shields.io/docker/pulls/pdok/azure-storage-usage-exporter.svg)](https://hub.docker.com/r/pdok/azure-storage-usage-exporter)

This app generates and exports/exposes statistics about cloud storage usage.

* The cloud storage used is [Azure Blob storage](https://azure.microsoft.com/en-us/products/storage/blobs). 
* Usage concerns (current) occupation, not transactions.

It was initially tailored to PDOK's use case to divide the usage of a multi-tenanted storage into:

* container (type of usage, not the storage container name per se)
* owner (tenant)
* dataset (a subdivision of owner/tenant)

It does so by:

* Periodically aggregating an [Azure Storage blob inventory report](https://learn.microsoft.com/en-us/azure/storage/blobs/blob-inventory).
* Applying ordered rules (regular expressions) to match container/directory/prefix to labels.
* Exposing the results as a Prometheus metrics endpoint. (Later used in a Grafana dashboard.)

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
   --config value                           Config file with aggregation labels and rules [$CONFIG]
   --help, -h                               show help
```

### Config file

Example config file:

```yaml
labels: # labels that are used in each metric and their default values
  type: other
  tenant: other
rules: # rules are tried in order until a pattern matches
  - pattern: ^strange-dir/(?P<tenant>[^/]+)/.+
    labels: # static labels that don't get their values from the regex 
      - type: special
  - pattern: ^(?P<type>[^/]+)/(?P<tenant>[^/]+)/.+
```

### Observability

#### Health checks

Health endpoint is available on `/health`.

### Linting

Install [golangci-lint](https://golangci-lint.run/usage/install/) and run `golangci-lint run`
from the root.
