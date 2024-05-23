package metrics

import (
	"log"
	"strconv"
	"time"

	"github.com/creasty/defaults"

	"github.com/PDOK/azure-storage-usage-exporter/internal/agg"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type Updater struct {
	config            Config
	aggregator        *agg.Aggregator
	storageUsageGauge *prometheus.GaugeVec
	lastRunDateMetric prometheus.Gauge
	lastRunDate       time.Time
}

type Config struct {
	MetricNamespace string `yaml:"metricNamespace" default:"azure"`
	MetricSubsystem string `yaml:"metricSubsystem" default:"storage"`
	Limit           int    `yaml:"limit" default:"1000"`
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

func NewUpdater(aggregator *agg.Aggregator, config Config) *Updater {
	return &Updater{
		config:     config,
		aggregator: aggregator,
		// promauto automatically registers with prometheus.DefaultRegisterer
		storageUsageGauge: promauto.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: config.MetricNamespace,
			Subsystem: config.MetricSubsystem,
			Name:      "usage",
		}, aggregator.GetLabelNames()),
		lastRunDateMetric: promauto.NewGauge(prometheus.GaugeOpts{
			Namespace: config.MetricNamespace,
			Subsystem: config.MetricSubsystem,
			Name:      "last_run_date",
		}),
	}
}

func (ms *Updater) UpdatePromMetrics() error {
	log.Printf("start updating metrics. previous run was %s", ms.lastRunDate)
	aggregationResults, lastRunDate, err := ms.aggregator.Aggregate(ms.lastRunDate)
	if err != nil {
		if !lastRunDate.IsZero() && lastRunDate.Equal(ms.lastRunDate) {
			log.Print("no newer blob inventory run found")
			return nil
		}
		return err
	}

	log.Print("start setting metrics")
	ms.lastRunDate = lastRunDate
	ms.lastRunDateMetric.Set(float64(lastRunDate.UnixNano()) / 1e9)
	ms.storageUsageGauge.Reset()

	if len(aggregationResults) > ms.config.Limit {
		log.Printf("(metrics count will be limited to %d (of %d)", ms.config.Limit, len(aggregationResults))
	}
	for i, aggregationResult := range aggregationResults {
		if i >= ms.config.Limit {
			break
		}
		ms.storageUsageGauge.With(aggregationGroupToLabels(aggregationResult.AggregationGroup)).Set(float64(aggregationResult.StorageUsage))
	}
	log.Printf("done updating metrics for run %s", ms.lastRunDate)

	return nil
}

func aggregationGroupToLabels(aggregationGroup agg.AggregationGroup) prometheus.Labels {
	labels := aggregationGroup.Labels
	labels[agg.Deleted] = strconv.FormatBool(aggregationGroup.Deleted)
	return labels
}
