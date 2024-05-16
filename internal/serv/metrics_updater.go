package serv

import (
	"strconv"
	"time"

	"github.com/PDOK/azure-storage-usage-exporter/internal/agg"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/tobshub/go-sortedmap"
)

type MetricsUpdater struct {
	aggregator        *agg.Aggregator
	storageUsageGauge *prometheus.GaugeVec
	lastRunDateMetric prometheus.Gauge
	lastRunDate       time.Time
	limit             int
}

var (
	varLabelNames = []string{
		agg.Container,
		agg.Owner,
		agg.Dataset,
		agg.Deleted,
	}
)

func NewMetricsUpdater(aggregator *agg.Aggregator, metricNamespace, metricSubsystem string, limit int) *MetricsUpdater {
	return &MetricsUpdater{
		aggregator: aggregator,
		// promauto automatically registers with prometheus.DefaultRegisterer
		storageUsageGauge: promauto.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: metricNamespace,
			Subsystem: metricSubsystem,
			Name:      "usage",
		}, varLabelNames),
		lastRunDateMetric: promauto.NewGauge(prometheus.GaugeOpts{
			Namespace: metricNamespace,
			Subsystem: metricSubsystem,
			Name:      "lastRunDateMetric",
		}),
		limit: limit,
	}
}

func (ms *MetricsUpdater) UpdatePromMetrics() error {
	aggregationResults, lastRunDate, err := ms.aggregator.Aggregate(ms.lastRunDate)
	if err != nil {
		if lastRunDate.Equal(ms.lastRunDate) {
			return nil // no update
		}
		return err
	}

	ms.lastRunDate = lastRunDate
	ms.lastRunDateMetric.Set(float64(lastRunDate.UnixNano()) / 1e9)
	ms.storageUsageGauge.Reset()

	i := 0
	aggregationResults.IterFunc(false, func(rec sortedmap.Record[agg.AggregationGroup, agg.StorageUsage]) bool {
		// math.MaxInt64 < math.MaxFloat64
		// and the larger the value, the least significant digits (that might get lost) become less significant anyway
		ms.storageUsageGauge.With(aggregationGroupToLabels(rec.Key)).Set(float64(rec.Val))
		i++
		return i < ms.limit // break on limit
	})

	return nil
}

func aggregationGroupToLabels(aggregationGroup agg.AggregationGroup) prometheus.Labels {
	return map[string]string{
		agg.Container: aggregationGroup.Container,
		agg.Owner:     aggregationGroup.Owner,
		agg.Dataset:   aggregationGroup.Dataset,
		agg.Deleted:   strconv.FormatBool(aggregationGroup.Deleted),
	}
}
