package agg

import (
	"context"
	"errors"
	"fmt"
	"log"
	"slices"
	"time"

	"github.com/tobshub/go-sortedmap"

	"github.com/jmoiron/sqlx"
	_ "github.com/marcboeker/go-duckdb" // duckdb sql driver

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/oriser/regroup"
	"golang.org/x/exp/maps"
)

const (
	runDatePathFormat  = "2006/01/02/15-04-05"
	maxSaneCountDuRows = 1e7
)

const (
	Container  = "container"
	Owner      = "owner"
	Dataset    = "dataset"
	Deleted    = "deleted"
	OtherValue = "OTHER"
)

var (
	blobInventoryFileRunMatchPattern = regroup.MustCompile(`^(?P<date>\d{4}/\d{2}/\d{2}/\d{2}-\d{2}-\d{2})/(?P<rule>[^/]+)/[^_]+_\d+_\d+.parquet$`)
	otherAggregationGroup            = AggregationGroup{
		Container: OtherValue,
		Owner:     OtherValue,
		Dataset:   OtherValue,
	}
)

type Aggregator struct {
	azureStorageConnectionString string
	blobInventoryContainer       string
	rules                        []AggregationRule
}

// TODO maybe refactor to not have hard coded labels
type AggregationGroup struct {
	Container string `yaml:"container,omitempty" regroup:"container"`
	Owner     string `yaml:"owner,omitempty" regroup:"owner"`
	Dataset   string `yaml:"dataset,omitempty" regroup:"dataset"`
	Deleted   bool   `yaml:"deleted,omitempty"`
}

// StorageUsage is storage usage/size in bytes
type StorageUsage = int64

type AggregationResults = *sortedmap.SortedMap[AggregationGroup, StorageUsage]

// AggregationRule is matched to a blob's path (including container/bucket, without filename, maximum X levels deep).
// If it matches, the named groups from the regex pattern or the defaults from the AggregationGroup are used
// to aggregate the blob's size cq storage usage.
type AggregationRule struct {
	AggregationGroup
	Pattern ReGroup `yaml:"pattern"`
}

type duRow struct {
	Dir     string       `db:"dir"`
	Deleted *bool        `db:"deleted"`
	Bytes   StorageUsage `db:"bytes"`
	Count   int64        `db:"cnt"`
}

type rulesRanByDate = map[time.Time][]string

func NewAggregator(azureStorageConnectionString, blobInventoryContainer string, aggregationRules []AggregationRule) *Aggregator {
	return &Aggregator{
		azureStorageConnectionString: azureStorageConnectionString,
		blobInventoryContainer:       blobInventoryContainer,
		rules:                        aggregationRules,
	}
}

func (a *Aggregator) Aggregate(minimalLastRunDate time.Time) (aggregationResults AggregationResults, lastRunDate time.Time, err error) {
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

func (a *Aggregator) aggregateRun(runDate time.Time) (AggregationResults, error) {
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
	SELECT array_to_string(string_split(i.Name, '/')[1:-2][1:4], '/') as dir, -- it's a 1-based index; inclusive boundaries; :-2 strips the filename
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
	rows, err := db.Queryx(duQuery, parquetWildcardPath, maxSaneCountDuRows)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	aggregationResult := sortedmap.New[AggregationGroup, StorageUsage](0, func(i, j StorageUsage) bool {
		return i > j // desc
	})
	var row duRow
	i := 0
	for rows.Next() {
		err = rows.StructScan(&row)
		if err != nil {
			return nil, err
		}
		aggregationGroup := a.applyRulesToAggregate(row)
		soFar, _ := aggregationResult.Get(aggregationGroup)
		aggregationResult.Insert(aggregationGroup, soFar+row.Bytes)
		if i%10000 == 0 {
			log.Printf("%d du rows processed so far", i)
		}
		i++
	}
	log.Printf("done aggregating/querying blob inventory, %d du rows processed", i)
	if i >= maxSaneCountDuRows {
		return nil, errors.New("du rows count sanity limit was reached")
	}

	return aggregationResult, nil
}

func (a *Aggregator) applyRulesToAggregate(row duRow) AggregationGroup {
	for _, aggregationRule := range a.rules {
		aggregationGroup := &AggregationGroup{}
		err := aggregationRule.Pattern.MatchToTarget(row.Dir, aggregationGroup)
		if err != nil {
			continue
		}
		aggregationGroup.applyRuleDefaults(aggregationRule)
		if row.Deleted != nil && *row.Deleted {
			aggregationGroup.Deleted = true
		}
		return *aggregationGroup
	}
	return otherAggregationGroup
}

func (ag *AggregationGroup) applyRuleDefaults(rule AggregationRule) {
	ag.Container = defaultStr(defaultStr(ag.Container, rule.Container), OtherValue)
	ag.Owner = defaultStr(defaultStr(ag.Owner, rule.Owner), OtherValue)
	ag.Dataset = defaultStr(defaultStr(ag.Dataset, rule.Dataset), OtherValue)
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

func defaultStr(s, d string) string {
	if s == "" {
		return d
	}
	return s
}
