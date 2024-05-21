package agg

import (
	"encoding/json"
	"errors"
	"log"
	"slices"
	"time"

	"github.com/PDOK/azure-storage-usage-exporter/internal/du"

	_ "github.com/marcboeker/go-duckdb" // duckdb sql driver

	"golang.org/x/exp/maps"
)

const (
	Deleted = "deleted"
)

type Labels = map[string]string

type AggregationRule struct {
	// The named groups are used as labels
	Pattern ReGroup `yaml:"pattern"`
	// A label not found as named group is looked up in this
	StaticLabels map[string]string `yaml:"labels"`
}

type AggregationGroup struct {
	Labels  Labels
	Deleted bool
}

type AggregationResult struct {
	AggregationGroup AggregationGroup
	StorageUsage     du.StorageUsage
}

type Aggregator struct {
	duReader           du.Reader
	labelsWithDefaults Labels
	rules              []AggregationRule
}

func NewAggregator(duReader du.Reader, labels Labels, rules []AggregationRule) (*Aggregator, error) {
	if _, exists := labels[Deleted]; exists {
		return nil, errors.New("cannot use deleted as a label")
	}
	return &Aggregator{
		duReader:           duReader,
		labelsWithDefaults: labels,
		rules:              rules,
	}, nil
}

func (a *Aggregator) GetLabelNames() []string {
	keys := maps.Keys(a.labelsWithDefaults)
	keys = append(keys, Deleted)
	return keys
}

func (a *Aggregator) Aggregate(previousRunDate time.Time) (aggregationResults []AggregationResult, runDate time.Time, err error) {
	log.Print("starting aggregation")
	runDate, rowsCh, errCh, err := a.duReader.Read(previousRunDate)
	if err != nil {
		return nil, runDate, err
	}
	if !runDate.After(previousRunDate) {
		return nil, runDate, nil
	}

	intermediateResults := make(map[string]du.StorageUsage)
	i := 0
	for rowsCh != nil && errCh != nil {
		select {
		case err, ok := <-errCh:
			if !ok {
				errCh = nil
				continue
			}
			if err != nil {
				return nil, runDate, err
			}
		case row, ok := <-rowsCh:
			if !ok {
				rowsCh = nil
				continue
			}
			aggregationGroup := a.applyRulesToAggregate(row)
			intermediateResults[marshalAggregationGroup(aggregationGroup)] += row.Bytes
			if i%10000 == 0 {
				log.Printf("%d du rows processed so far", i)
			}
			i++
		}
	}
	log.Printf("done aggregating blob inventory, %d du rows processed", i)

	return intermediateResultsToAggregationResults(intermediateResults), runDate, nil
}

// The key in intermediate results of Aggregator.Aggregate is a JSON representation of AggregationGroup
// because a map is not a comparable type.
// Property order in the JSON is predictable/constant.
func marshalAggregationGroup(aggregationGroup AggregationGroup) string {
	b, _ := json.Marshal(aggregationGroup)
	return string(b)
}

func unmarshalAggregationGroup(aggregationGroupJSON string) AggregationGroup {
	aggregationGroup := new(AggregationGroup)
	_ = json.Unmarshal([]byte(aggregationGroupJSON), aggregationGroup)
	return *aggregationGroup
}

func intermediateResultsToAggregationResults(intermediateResults map[string]du.StorageUsage) []AggregationResult {
	aggregationResults := make([]AggregationResult, len(intermediateResults))
	i := 0
	for aggregationGroup, storageUsage := range intermediateResults {
		aggregationResults[i] = AggregationResult{
			AggregationGroup: unmarshalAggregationGroup(aggregationGroup),
			StorageUsage:     storageUsage,
		}
		i++
	}

	// sort by storageUsage desc
	slices.SortFunc(aggregationResults, func(a, b AggregationResult) int {
		return int(b.StorageUsage - a.StorageUsage)
	})

	return aggregationResults
}

func (a *Aggregator) applyRulesToAggregate(row du.Row) AggregationGroup {
	for _, aggregationRule := range a.rules {
		labelsFromPattern, err := aggregationRule.Pattern.Groups(row.Dir)
		if err != nil {
			continue
		}
		aggregationGroup := AggregationGroup{
			Labels: a.applyRuleDefaults(labelsFromPattern, aggregationRule),
		}
		aggregationGroup.Deleted = nilBoolToBool(row.Deleted)
		return aggregationGroup
	}
	return AggregationGroup{
		Labels:  a.labelsWithDefaults,
		Deleted: nilBoolToBool(row.Deleted),
	}
}

func (a *Aggregator) applyRuleDefaults(labelsFromPattern Labels, rule AggregationRule) Labels {
	labels := maps.Clone(a.labelsWithDefaults)
	for label, defaultVal := range labels {
		labels[label] = defaultStr(
			labelsFromPattern[label], // first use a match group
			rule.StaticLabels[label], // otherwise use a static label from the rule
			defaultVal,               // fall back to the label default
		)
	}
	return labels
}

func defaultStr(s ...string) string {
	for i := range s {
		if s[i] != "" {
			return s[i]
		}
	}
	return ""
}

func nilBoolToBool(p *bool) bool {
	if p != nil {
		return *p
	}
	return false
}
