package agg

import (
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/jmoiron/sqlx"
	_ "github.com/marcboeker/go-duckdb" // duckdb sql driver

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/oriser/regroup"
	"golang.org/x/exp/maps"
)

const (
	runDatePathFormat = "2006/01/02/15-04-05"
	other             = "OTHER"
)

var (
	blobInventoryFileRunMatchPattern = regroup.MustCompile(`^(?P<date>\d{4}/\d{2}/\d{2}/\d{2}-\d{2}-\d{2})/(?P<rule>[^/]+)/[^_]+_\d+_\d+.parquet$`)
	otherAggregationGroup            = AggregationGroup{
		Container: other,
		Owner:     other,
		Dataset:   other,
	}
)

type Aggregator struct {
	AzureStorageConnectionString string
	BlobInventoryContainer       string
	Rules                        []AggregationRule
}

type AggregationGroup struct {
	Container string `yaml:"container,omitempty" regroup:"container"`
	Owner     string `yaml:"owner,omitempty" regroup:"owner"`
	Dataset   string `yaml:"dataset,omitempty" regroup:"dataset"`
}

// StorageUsage is storage usage/size in bytes
type StorageUsage int64

type AggregationResult map[AggregationGroup]StorageUsage

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

type rulesRanByDate map[time.Time][]string

func (a *Aggregator) Aggregate() (AggregationResult, error) {
	rulesRanByDate, err := a.findRuns()
	if err != nil {
		return nil, err
	}
	lastRunDate, found := getLastRunDate(rulesRanByDate)
	if !found {
		return nil, nil
	}
	aggregationResult, err := a.doTheWork(lastRunDate)
	if err != nil {
		return nil, err
	}

	return aggregationResult, nil
}

func (a *Aggregator) doTheWork(runDate time.Time) (AggregationResult, error) {
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
	LIMIT 100000 -- sanity limit
	`
	parquetWildcardPath := fmt.Sprintf("az://%s/%s/%s/*.parquet", a.BlobInventoryContainer, runDate.Format(runDatePathFormat), "*")
	rows, err := db.Queryx(duQuery, parquetWildcardPath)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	aggregationResult := make(AggregationResult)
	var row duRow
	for rows.Next() {
		err = rows.StructScan(&row)
		if err != nil {
			return nil, err
		}
		aggregationGroup := a.applyRulesToAggregate(row)
		// TODO something with deleted
		aggregationResult[aggregationGroup] += row.Bytes
	}

	return aggregationResult, nil
}

func (a *Aggregator) applyRulesToAggregate(row duRow) AggregationGroup {
	for _, aggregationRule := range a.Rules {
		aggregationGroup := &AggregationGroup{}
		err := aggregationRule.Pattern.MatchToTarget(row.Dir, aggregationGroup)
		if err != nil {
			continue
		}
		aggregationGroup.applyRuleDefaults(aggregationRule)
		return *aggregationGroup
	}
	return otherAggregationGroup
}

func (ag *AggregationGroup) applyRuleDefaults(rule AggregationRule) {
	ag.Container = defaultStr(defaultStr(ag.Container, rule.Container), other)
	ag.Owner = defaultStr(defaultStr(ag.Owner, rule.Owner), other)
	ag.Dataset = defaultStr(defaultStr(ag.Dataset, rule.Dataset), other)
}

func (a *Aggregator) initDB(db *sqlx.DB) error {
	// language=sql
	azInitQuery := `INSTALL azure;
					LOAD azure;
					SET azure_transport_option_type = 'curl'; -- fixes cert issues
					CREATE SECRET az (TYPE AZURE, PROVIDER CONFIG, CONNECTION_STRING '%s');`
	azInitQuery = fmt.Sprintf(azInitQuery, a.AzureStorageConnectionString) // FIXME db.NamedExec
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
	blobClient, err := azblob.NewClientFromConnectionString(a.AzureStorageConnectionString, nil)
	if err != nil {
		return nil, err
	}
	pager := blobClient.NewListBlobsFlatPager(a.BlobInventoryContainer, nil)
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
