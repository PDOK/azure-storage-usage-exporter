package agg

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"slices"
	"time"

	"github.com/jmoiron/sqlx"
	_ "github.com/marcboeker/go-duckdb" // duckdb sql driver

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/oriser/regroup"
	"golang.org/x/exp/maps"
)

const (
	runDatePathFormat  = "2006/01/02/15-04-05"
	maxSaneCountDuRows = 1e7
	duDepth            = 4
)

const (
	Deleted = "deleted"
)

var (
	blobInventoryFileRunMatchPattern = regroup.MustCompile(`^(?P<date>\d{4}/\d{2}/\d{2}/\d{2}-\d{2}-\d{2})/(?P<rule>[^/]+)/[^_]+_\d+_\d+.parquet$`)
)

// StorageUsage is storage usage/size in bytes
type StorageUsage = int64

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
	StorageUsage     StorageUsage
}

type Aggregator struct {
	azureStorageConnectionString string
	blobInventoryContainer       string
	labelsWithDefaults           Labels
	rules                        []AggregationRule
}

type duRow struct {
	Dir     string       `db:"dir"`
	Deleted *bool        `db:"deleted"`
	Bytes   StorageUsage `db:"bytes"`
	Count   int64        `db:"cnt"`
}

type rulesRanByDate = map[time.Time][]string

func NewAggregator(azureStorageConnectionString, blobInventoryContainer string, labels Labels, rules []AggregationRule) *Aggregator {
	return &Aggregator{
		azureStorageConnectionString: azureStorageConnectionString,
		blobInventoryContainer:       blobInventoryContainer,
		labelsWithDefaults:           labels,
		rules:                        rules,
	}
}

func (a *Aggregator) GetLabelNames() []string {
	keys := maps.Keys(a.labelsWithDefaults)
	keys = append(keys, Deleted)
	return keys
}

func (a *Aggregator) Aggregate(minimalLastRunDate time.Time) (aggregationResults []AggregationResult, lastRunDate time.Time, err error) {
	log.Print("finding last inventory run")
	rulesRanByDate, err := a.findRuns()
	if err != nil {
		return
	}
	lastRunDate, found := getLastRunDate(rulesRanByDate)
	if !found {
		err = errors.New("no last run date found")
		return
	}
	if !lastRunDate.After(minimalLastRunDate) {
		err = errors.New("last run date is too old")
		return
	}
	log.Print("starting aggregation")
	aggregationResults, err = a.aggregateRun(lastRunDate)
	if err != nil {
		return
	}
	return
}

// aggregateRun first coarsely aggregates with duckdb to group all blob names to max X levels deep
// and the resulting "du" rows are then further aggregated using the aggregation rules bases on regexes
func (a *Aggregator) aggregateRun(runDate time.Time) ([]AggregationResult, error) {
	log.Print("setting up duckdb / azure blob store connection")
	db, err := sqlx.Connect("duckdb", "")
	if err != nil {
		return nil, err
	}
	defer db.Close()
	err = a.initDB(db)
	if err != nil {
		return nil, err
	}

	// language=sql
	duQuery := `
	SELECT array_to_string(string_split(i.Name, '/')[1:-2][1:?], '/') as dir, -- it's a 1-based index; inclusive boundaries; :-2 strips the filename
		   i."Deleted" as deleted,
		   sum(i."Content-Length") as bytes,
		   count(*) as cnt
	FROM read_parquet([?]) i
	GROUP BY dir, deleted
	ORDER BY bytes DESC
	LIMIT ? -- sanity limit
	`
	parquetWildcardPath := fmt.Sprintf("az://%s/%s/%s/*.parquet", a.blobInventoryContainer, runDate.Format(runDatePathFormat), "*")

	log.Print("start aggregating/querying blob inventory (might take a while)")
	rows, err := db.Queryx(duQuery, duDepth, parquetWildcardPath, maxSaneCountDuRows)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	intermediateResults := make(map[string]StorageUsage)
	var row duRow
	i := 0
	for rows.Next() {
		err = rows.StructScan(&row)
		if err != nil {
			return nil, err
		}
		aggregationGroup := a.applyRulesToAggregate(row)
		intermediateResults[marshalAggregationGroup(aggregationGroup)] += row.Bytes
		if i%10000 == 0 {
			log.Printf("%d du rows processed so far", i)
		}
		i++
	}
	log.Printf("done aggregating/querying blob inventory, %d du rows processed", i)
	if i >= maxSaneCountDuRows {
		return nil, errors.New("du rows count sanity limit was reached")
	}

	return intermediateResultsToAggregationResults(intermediateResults), nil
}

// The key in intermediate results of aggregateRun is a JSON representation of AggregationGroup
// because a map is not a comparable type.
// Property order in the JSON is predictable/constant.
func marshalAggregationGroup(aggregationGroup AggregationGroup) string {
	b, _ := json.Marshal(aggregationGroup)
	return string(b)
}

func unmarshalAggregationGroup(aggregationGroupJson string) AggregationGroup {
	aggregationGroup := new(AggregationGroup)
	_ = json.Unmarshal([]byte(aggregationGroupJson), aggregationGroup)
	return *aggregationGroup
}

func intermediateResultsToAggregationResults(intermediateResults map[string]StorageUsage) []AggregationResult {
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

func (a *Aggregator) applyRulesToAggregate(row duRow) AggregationGroup {
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
	labels := a.labelsWithDefaults
	for label, defaultVal := range labels {
		labels[label] = defaultStr(
			labelsFromPattern[label], // first use a match group
			rule.StaticLabels[label], // otherwise use a static label from the rule
			defaultVal,               // fall back to the label default
		)
	}
	return labels
}

func (a *Aggregator) initDB(db *sqlx.DB) error {
	// language=sql
	azInitQuery := `INSTALL azure;
					LOAD azure;
					SET azure_transport_option_type = 'curl'; -- fixes cert issues
					CREATE SECRET az (TYPE AZURE, PROVIDER CONFIG, CONNECTION_STRING '%s');`
	azInitQuery = fmt.Sprintf(azInitQuery, a.azureStorageConnectionString) // FIXME db.NamedExec
	if _, err := db.Exec(azInitQuery); err != nil {
		return err
	}

	// language=sql
	memSetQuery := `SET memory_limit = '15GB'; -- TODO cli options for resources
	 				SET max_memory = '15GB';
					SET threads = 4;`
	if _, err := db.Exec(memSetQuery); err != nil {
		return err
	}

	return nil
}

func (a *Aggregator) findRuns() (rulesRanByDate, error) {
	blobClient, err := azblob.NewClientFromConnectionString(a.azureStorageConnectionString, nil)
	if err != nil {
		return nil, err
	}
	pager := blobClient.NewListBlobsFlatPager(a.blobInventoryContainer, nil)
	rulesRanByDate := make(map[time.Time][]string)
	for pager.More() {
		page, err := pager.NextPage(context.TODO())
		if err != nil {
			return nil, err
		}
		for _, blob := range page.Segment.BlobItems {
			g, err := blobInventoryFileRunMatchPattern.Groups(*blob.Name)
			if err != nil { // no match
				continue
			}
			runDate, err := time.Parse(runDatePathFormat, g["date"])
			if err != nil { // unexpected
				return nil, err
			}
			rulesRanByDate[runDate] = append(rulesRanByDate[runDate], g["rule"])
		}
	}
	return rulesRanByDate, nil
}

func getLastRunDate(rulesRanByDate rulesRanByDate) (runDate time.Time, ok bool) {
	dates := maps.Keys(rulesRanByDate)
	if len(dates) == 0 {
		return
	}
	return slices.MaxFunc(dates, func(i, j time.Time) int {
		return i.Compare(j)
	}), true
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
