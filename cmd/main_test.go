package main

import (
	"os"
	"testing"

	"github.com/PDOK/azure-storage-usage-exporter/internal/agg"
	"github.com/PDOK/azure-storage-usage-exporter/internal/du"
	"github.com/PDOK/azure-storage-usage-exporter/internal/metrics"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v2"
)

func TestPerf(t *testing.T) {
	t.Skip("local")
	t.Run("perf", func(t *testing.T) {
		configFile, err := os.ReadFile("example/pdok-config.yaml")
		require.Nil(t, err)
		config := new(Config)
		err = yaml.Unmarshal(configFile, config)
		require.Nil(t, err)
		config.Azure.AzureStorageConnectionString = os.Getenv("AZURE_STORAGE_CONNECTION_STRING")
		duReader := du.NewAzureBlobInventoryReportDuReader(*config.Azure)
		aggregator, err := agg.NewAggregator(duReader, config.Labels, config.Rules)
		require.Nil(t, err)
		updater := metrics.NewUpdater(aggregator, config.Metrics)

		err = updater.UpdatePromMetrics()
		require.Nil(t, err)
	})
}
