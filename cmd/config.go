package main

import (
	"github.com/PDOK/azure-storage-usage-exporter/internal/agg"
	"github.com/PDOK/azure-storage-usage-exporter/internal/du"
	"github.com/PDOK/azure-storage-usage-exporter/internal/metrics"
	"github.com/creasty/defaults"
)

type Config struct {
	Azure   *du.AzureBlobInventoryReportConfig `yaml:"azure,omitempty"`
	Metrics metrics.Config                     `yaml:"metrics,omitempty"`
	Labels  agg.Labels                         `yaml:"labels"`
	Rules   []agg.AggregationRule              `yaml:"rules"`
}

type unmarshalledConfig Config

func (c *Config) UnmarshalYAML(unmarshal func(any) error) error {
	tmp := new(unmarshalledConfig)
	if err := defaults.Set(tmp); err != nil {
		return err
	}
	if err := unmarshal(tmp); err != nil {
		return err
	}
	*c = Config(*tmp)
	return nil
}
