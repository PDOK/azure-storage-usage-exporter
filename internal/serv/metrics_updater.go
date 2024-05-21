package serv

import (
	"log"
	"strconv"
	"time"

	"github.com/PDOK/azure-storage-usage-exporter/internal/agg"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type MetricsUpdater struct {
	aggregator        *agg.Aggregator
	storageUsageGauge *prometheus.GaugeVec
	lastRunDateMetric prometheus.Gauge
	lastRunDate       time.Time
	limit             int
}

func NewMetricsUpdater(aggregator *agg.Aggregator, metricNamespace, metricSubsystem string, limit int) *MetricsUpdater {
	return &MetricsUpdater{
		aggregator: aggregator,
		// promauto automatically registers with prometheus.DefaultRegisterer
		storageUsageGauge: promauto.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: metricNamespace,
			Subsystem: metricSubsystem,
			Name:      "usage",
		}, aggregator.GetLabelNames()),
		lastRunDateMetric: promauto.NewGauge(prometheus.GaugeOpts{
			Namespace: metricNamespace,
			Subsystem: metricSubsystem,
			Name:      "lastRunDateMetric",
		}),
		limit: limit,
	}
}

func (ms *MetricsUpdater) UpdatePromMetrics() error {
	log.Printf("start updating metrics. last run was %s", ms.lastRunDate)
	aggregationResults, lastRunDate, err := ms.aggregator.Aggregate(ms.lastRunDate)
	if err != nil {
		if !lastRunDate.IsZero() && lastRunDate.Equal(ms.lastRunDate) {
			log.Print("no newer blob inventory fun found")
			return nil
		}
		return err
	}

	log.Print("start setting metrics")
	ms.lastRunDate = lastRunDate
	ms.lastRunDateMetric.Set(float64(lastRunDate.UnixNano()) / 1e9)
	ms.storageUsageGauge.Reset()

	if len(aggregationResults) > ms.limit {
		log.Printf("(metrics count will be limited to %d (of %d)", ms.limit, len(aggregationResults))
	}
	for i, aggregationResult := range aggregationResults {
		if i >= ms.limit {
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
