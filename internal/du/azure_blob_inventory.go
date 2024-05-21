package du

import (
	"context"
	"errors"
	"fmt"
	"log"
	"slices"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/jmoiron/sqlx"
	"github.com/oriser/regroup"
	"golang.org/x/exp/maps"
)

type AzureBlobInventoryReportDuReader struct {
	azureStorageConnectionString string
	blobInventoryContainer       string
}

type rulesRanByDate = map[time.Time][]string

const (
	runDatePathFormat  = "2006/01/02/15-04-05"
	maxSaneCountDuRows = 1e7
	duDepth            = 4
)

var (
	blobInventoryFileRunMatchPattern = regroup.MustCompile(`^(?P<date>\d{4}/\d{2}/\d{2}/\d{2}-\d{2}-\d{2})/(?P<rule>[^/]+)/[^_]+_\d+_\d+.parquet$`)
)

func NewAzureBlobInventoryReportDuReader(azureStorageConnectionString, blobInventoryContainer string) *AzureBlobInventoryReportDuReader {
	return &AzureBlobInventoryReportDuReader{
		azureStorageConnectionString: azureStorageConnectionString,
		blobInventoryContainer:       blobInventoryContainer,
	}
}

func (ar *AzureBlobInventoryReportDuReader) Read(previousRunDate time.Time) (time.Time, <-chan Row, <-chan error, error) {
	log.Print("finding last inventory run")
	rulesRanByDate, err := ar.findRuns()
	if err != nil {
		return time.Time{}, nil, nil, err
	}
	runDate, found := getLastRunDate(rulesRanByDate)
	if !found {
		err = errors.New("no last run date found")
		return runDate, nil, nil, err
	}
	if !runDate.After(previousRunDate) { // no new data
		err = errors.New("last run date is not after previous run date")
		return runDate, nil, nil, err
	}

	log.Print("setting up duckdb / azure blob store connection")
	db, err := sqlx.Connect("duckdb", "")
	if err != nil {
		return runDate, nil, nil, err
	}
	err = ar.initDB(db)
	if err != nil {
		return runDate, nil, nil, err
	}

	rowsReceiver := make(chan Row, maxSaneCountDuRows/100)
	errReceiver := make(chan error)
	go ar.readRowsFromInventoryReport(runDate, db, rowsReceiver, errReceiver)

	return runDate, rowsReceiver, errReceiver, nil
}

// readRowsFromInventoryReport coarsely aggregates the inventory reports parquet output with duckdb,
// grouping all blob names to max duDepth levels deep
func (ar *AzureBlobInventoryReportDuReader) readRowsFromInventoryReport(runDate time.Time, db *sqlx.DB, rowsCh chan<- Row, errCh chan<- error) {
	defer close(rowsCh)
	defer close(errCh)

	// language=sql
	duQuery := `
	SELECT array_to_string(string_split(i.Name, '/')[1:-2][1:?], '/') as dir, -- it's ar 1-based index; inclusive boundaries; :-2 strips the filename
		   i."Deleted" as deleted,
		   sum(i."Content-Length") as bytes,
		   count(*) as cnt
	FROM read_parquet([?]) i
	GROUP BY dir, deleted
	ORDER BY bytes DESC
	LIMIT ? -- sanity limit
	`
	parquetWildcardPath := fmt.Sprintf("az://%s/%s/%s/*.parquet", ar.blobInventoryContainer, runDate.Format(runDatePathFormat), "*")

	log.Print("start querying blob inventory (might take a while)")
	dbRows, err := db.Queryx(duQuery, duDepth, parquetWildcardPath, maxSaneCountDuRows)
	if err != nil {
		errCh <- err
		return
	}
	defer dbRows.Close()
	i := 0
	for dbRows.Next() {
		if i >= maxSaneCountDuRows {
			errCh <- errors.New("du rows count sanity limit was reached")
			return
		}
		var duRow Row
		err = dbRows.StructScan(&duRow)
		if err != nil {
			errCh <- err
			return
		}
		rowsCh <- duRow
		i++
	}
	log.Printf("done querying blob inventory, %d du rows processed", i)
}

func (ar *AzureBlobInventoryReportDuReader) initDB(db *sqlx.DB) error {
	// language=sql
	azInitQuery := `INSTALL azure;
					LOAD azure;
					SET azure_transport_option_type = 'curl'; -- fixes cert issues
					CREATE SECRET az (TYPE AZURE, PROVIDER CONFIG, CONNECTION_STRING '%s');`
	azInitQuery = fmt.Sprintf(azInitQuery, ar.azureStorageConnectionString) // FIXME db.NamedExec
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

func (ar *AzureBlobInventoryReportDuReader) findRuns() (rulesRanByDate, error) {
	blobClient, err := azblob.NewClientFromConnectionString(ar.azureStorageConnectionString, nil)
	if err != nil {
		return nil, err
	}
	pager := blobClient.NewListBlobsFlatPager(ar.blobInventoryContainer, nil)
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
