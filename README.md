# Azure storage usage exporter

[![Build](https://github.com/PDOK/azure-storage-usage-exporter/actions/workflows/build-and-publish-image.yml/badge.svg)](https://github.com/PDOK/azure-storage-usage-exporter/actions/workflows/build-and-publish-image.yml)
[![Lint (go)](https://github.com/PDOK/azure-storage-usage-exporter/actions/workflows/lint-go.yml/badge.svg)](https://github.com/PDOK/azure-storage-usage-exporter/actions/workflows/lint-go.yml)
[![Go Report Card](https://goreportcard.com/badge/github.com/PDOK/azure-storage-usage-exporter)](https://goreportcard.com/report/github.com/PDOK/azure-storage-usage-exporter)
[![Coverage (go)](https://github.com/PDOK/azure-storage-usage-exporter/wiki/coverage.svg)](https://raw.githack.com/wiki/PDOK/azure-storage-usage-exporter/coverage.html)
[![GitHub license](https://img.shields.io/github/license/PDOK/azure-storage-usage-exporter)](https://github.com/PDOK/azure-storage-usage-exporter/blob/master/LICENSE)
[![Docker Pulls](https://img.shields.io/docker/pulls/pdok/azure-storage-usage-exporter.svg)](https://hub.docker.com/r/pdok/azure-storage-usage-exporter)

This Prometheus exporter exposes statistics about [Azure Blob storage](https://azure.microsoft.com/en-us/products/storage/blobs) usage.
It relies upon data from [Azure Storage blob inventory reports](https://learn.microsoft.com/en-us/azure/storage/blobs/blob-inventory).
This data is aggregated, matched against configured labels and exposed as a Prometheus metrics endpoint.
The goal is to expose stats about storage/disk usage (not transactions) per Azure Blob container/directory/prefix.

## Example metrics output

```text
# HELP pdok_storage_lastRunDateMetric 
# TYPE pdok_storage_lastRunDateMetric gauge
pdok_storage_lastRunDateMetric 1.716122623e+09
# HELP pdok_storage_usage 
# TYPE pdok_storage_usage gauge
pdok_storage_usage{container="blob-inventory",dataset="other",deleted="false",owner="other"} 1.4511800263e+10
pdok_storage_usage{container="blob-inventory",dataset="other",deleted="true",owner="other"} 1.4697209865e+10
pdok_storage_usage{container="deliveries",dataset="something",deleted="false",owner="someone"} 1.4624738e+07
pdok_storage_usage{container="deliveries",dataset="something",deleted="true",owner="someone"} 2.0263731e+07
pdok_storage_usage{container="deliveries",dataset="somethingelse",deleted="false",owner="someoneelse"} 1.8042443e+07
# .....
```

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

### Linting

Install [golangci-lint](https://golangci-lint.run/usage/install/) and run `golangci-lint run`
from the root.
