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
# HELP azure_storage_last_run_date
# TYPE azure_storage_last_run_date gauge
azure_storage_last_run_date 1.716122623e+09
# HELP pdok_storage_usage 
# TYPE pdok_storage_usage gauge
azure_storage_usage{container="blob-inventory",dataset="other",deleted="false",owner="other",storage_account="devstoreaccount1"} 1.4511800263e+10
azure_storage_usage{container="blob-inventory",dataset="other",deleted="true",owner="other",storage_account="devstoreaccount1"} 1.4697209865e+10
azure_storage_usage{container="deliveries",dataset="something",deleted="false",owner="someone",storage_account="devstoreaccount1"} 1.4624738e+07
azure_storage_usage{container="deliveries",dataset="something",deleted="true",owner="someone",storage_account="devstoreaccount1"} 2.0263731e+07
azure_storage_usage{container="deliveries",dataset="somethingelse",deleted="false",owner="someoneelse",storage_account="devstoreaccount1"} 1.8042443e+07
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
   --azure-storage-connection-string value  Connection string for connecting to the Azure blob storage that holds the inventory (overrides the config file entry) [$AZURE_STORAGE_CONNECTION_STRING]
   --bind-address value                     The TCP network address addr that is listened on. (default: ":8080") [$BIND_ADDRESS]
   --config value                           Config file with aggregation labels and rules [$CONFIG]
   --help, -h                               show help
```

### Config file

Example config file:

```yaml
azure:
  azureStorageConnectionString: DefaultEndpointsProtocol=http;BlobEndpoint=http://localhost:10000/devstoreaccount1;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;
  blobInventoryContainer: blob-inventory
  maxMemory: 1GB
  threads: 4
metrics:
  metricNamespace: pdok
  metricSubsystem: storage
  limit: 1000
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
