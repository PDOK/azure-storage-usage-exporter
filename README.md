# PDOK storage usage exporter

[![Build](https://github.com/PDOK/azure-storage-usage-exporter/actions/workflows/build-and-publish-image.yml/badge.svg)](https://github.com/PDOK/azure-storage-usage-exporter/actions/workflows/build-and-publish-image.yml)
[![Lint (go)](https://github.com/PDOK/azure-storage-usage-exporter/actions/workflows/lint-go.yml/badge.svg)](https://github.com/PDOK/azure-storage-usage-exporter/actions/workflows/lint-go.yml)
[![Go Report Card](https://goreportcard.com/badge/github.com/PDOK/azure-storage-usage-exporter)](https://goreportcard.com/report/github.com/PDOK/azure-storage-usage-exporter)
[![Coverage (go)](https://github.com/PDOK/azure-storage-usage-exporter/wiki/coverage.svg)](https://raw.githack.com/wiki/PDOK/azure-storage-usage-exporter/coverage.html)
[![GitHub license](https://img.shields.io/github/license/PDOK/azure-storage-usage-exporter)](https://github.com/PDOK/azure-storage-usage-exporter/blob/master/LICENSE)
[![Docker Pulls](https://img.shields.io/docker/pulls/pdok/azure-storage-usage-exporter.svg)](https://hub.docker.com/r/pdok/azure-storage-usage-exporter)

This app generates and exports/exposes statistics about cloud storage usage.
It is tailored to PDOK's use case:

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

ToDo

### Configuration file

ToDo

### Observability

#### Health checks

Health endpoint is available on `/health`.

### Linting

Install [golangci-lint](https://golangci-lint.run/usage/install/) and run `golangci-lint run`
from the root.
