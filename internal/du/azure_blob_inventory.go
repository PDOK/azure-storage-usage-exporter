package du

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/url"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/creasty/defaults"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/jmoiron/sqlx"
	"github.com/oriser/regroup"
	"golang.org/x/exp/maps"
)

type AzureBlobInventoryReportConfig struct {
	AzureStorageConnectionString string `yaml:"AzureStorageConnectionString" default:"DefaultEndpointsProtocol=http;BlobEndpoint=http://localhost:10000/devstoreaccount1;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;"`
	BlobInventoryContainer       string `yaml:"BlobInventoryContainer" default:"blob-inventory"`
	MaxMemory                    string `yaml:"maxMemory" default:"1GB"`
	Threads                      int    `yaml:"threads" default:"4"`
}

type unmarshalledAzureBlobInventoryReportConfig AzureBlobInventoryReportConfig

func (c *AzureBlobInventoryReportConfig) UnmarshalYAML(unmarshal func(any) error) error {
	tmp := new(unmarshalledAzureBlobInventoryReportConfig)
	if err := defaults.Set(tmp); err != nil {
		return err
	}
	if err := unmarshal(tmp); err != nil {
		return err
	}
	*c = AzureBlobInventoryReportConfig(*tmp)
	return nil
}

type AzureBlobInventoryReportDuReader struct {
	config AzureBlobInventoryReportConfig
}

type rulesRanByDate = map[time.Time][]string

const (
	runDatePathFormat  = "2006/01/02/15-04-05"
	maxSaneCountDuRows = 10000000 // 10 million. if breached, maybe adapt duDepth
	duDepth            = 4        // aggregate blob usage 4 dirs deep
)

var (
	blobInventoryFileRunMatchPattern = regroup.MustCompile(`^(?P<date>\d{4}/\d{2}/\d{2}/\d{2}-\d{2}-\d{2})/(?P<rule>[^/]+)/[^_]+_\d+_\d+.parquet$`)
)

func NewAzureBlobInventoryReportDuReader(config AzureBlobInventoryReportConfig) *AzureBlobInventoryReportDuReader {
	return &AzureBlobInventoryReportDuReader{
		config: config,
	}
}

func (ar *AzureBlobInventoryReportDuReader) TestConnection() error {
	blobClient, err := ar.newBlobClient()
	if err != nil {
		return err
	}
	pager := blobClient.NewListBlobsFlatPager(ar.config.BlobInventoryContainer, &azblob.ListBlobsFlatOptions{MaxResults: int32Ptr(1)})
	_, err = pager.NextPage(context.TODO())
	return err
}

func (ar *AzureBlobInventoryReportDuReader) GetStorageAccountName() string {
	// github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/internal/shared/ParseConnectionString is unfortunately internal
	if match := regexp.MustCompile(`AccountName=([^;]+)`).FindStringSubmatch(ar.config.AzureStorageConnectionString); len(match) == 2 {
		return match[1]
	}
	if match := regexp.MustCompile(`BlobEndpoint=([^;]+)`).FindStringSubmatch(ar.config.AzureStorageConnectionString); len(match) == 2 {
		if blobEndpoint, err := url.Parse(match[1]); blobEndpoint != nil && err != nil {
			if blobEndpoint.Path != "" {
				return blobEndpoint.Path
			}
			return regexp.MustCompile(`^[^.]+`).FindString(blobEndpoint.Host)
		}
	}
	return "_unknown"
}

func (ar *AzureBlobInventoryReportDuReader) Read(previousRunDate time.Time) (time.Time, <-chan Row, <-chan error, error) {
	log.Print("finding newest inventory run")
	rulesRanByDate, err := ar.findRuns()
	if err != nil {
		return time.Time{}, nil, nil, err
	}
	runDate, found := getLastRunDate(rulesRanByDate)
	if !found {
		err = errors.New("no run date found")
		return runDate, nil, nil, err
	}
	if !runDate.After(previousRunDate) { // no new data
		err = errors.New("newest run date is not after previous run date")
		return runDate, nil, nil, err
	}
	log.Printf("found newest inventory run: %s", runDate)

	log.Print("setting up duckdb, including azure blob store connection")
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
	parquetWildcardPath := fmt.Sprintf("az://%s/%s/%s/*.parquet", ar.config.BlobInventoryContainer, runDate.Format(runDatePathFormat), "*")

	log.Print("start querying blob inventory (might take a while)")
	dbRows, err := db.Queryx(duQuery, duDepth, parquetWildcardPath, maxSaneCountDuRows) //nolint:sqlclosecheck // it's closed 5 lines down
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
	log.Printf("done querying blob inventory, %d disk usage rows processed", i)
}

func (ar *AzureBlobInventoryReportDuReader) initDB(db *sqlx.DB) error {
	// language=sql
	azInitQuery := `INSTALL azure;
					LOAD azure;
					SET azure_transport_option_type = 'curl'; -- fixes cert issues
					CREATE SECRET az (TYPE AZURE, PROVIDER CONFIG, CONNECTION_STRING '%s');`
	azInitQuery = fmt.Sprintf(azInitQuery, removeQuotes(ar.config.AzureStorageConnectionString))
	if _, err := db.Exec(azInitQuery); err != nil {
		return err
	}

	// language=sql
	memSetQuery := `SET max_memory = '%s';
					SET threads = %d;`
	memSetQuery = fmt.Sprintf(memSetQuery, removeQuotes(ar.config.MaxMemory), ar.config.Threads)
	if _, err := db.Exec(memSetQuery); err != nil {
		return err
	}

	return nil
}

func (ar *AzureBlobInventoryReportDuReader) findRuns() (rulesRanByDate, error) {
	blobClient, err := ar.newBlobClient()
	if err != nil {
		return nil, err
	}
	pager := blobClient.NewListBlobsFlatPager(ar.config.BlobInventoryContainer, nil)
	rulesRanByDate := make(map[time.Time][]string)
	for pager.More() {
		page, err := pager.NextPage(context.TODO())
		if err != nil {
			return nil, err
		}
		if page.Segment == nil {
			break
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

func (ar *AzureBlobInventoryReportDuReader) newBlobClient() (*azblob.Client, error) {
	blobClient, err := azblob.NewClientFromConnectionString(ar.config.AzureStorageConnectionString, nil)
	if err != nil {
		return nil, err
	}
	return blobClient, nil
}

func int32Ptr(i int32) *int32 {
	return &i
}

func removeQuotes(s string) string {
	return strings.ReplaceAll(s, "'", "")
}
