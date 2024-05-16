package main

import (
	"github.com/google/uuid"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/PDOK/azure-storage-usage-exporter/internal/serv"
	"github.com/go-co-op/gocron/v2"
	"github.com/iancoleman/strcase"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/PDOK/azure-storage-usage-exporter/internal/agg"

	"gopkg.in/yaml.v2"

	"github.com/urfave/cli/v2"
)

const (
	cliOptAzureStorageConnectionString = "azure-storage-connection-string"
	cliOptBindAddress                  = "bind-address"
	cliOptBlobInventoryContainer       = "blob-inventory-container"
	cliOptExtraRulesFile               = "extra-rules-file"
)

var (
	cliFlags = []cli.Flag{
		&cli.StringFlag{
			Name:     cliOptAzureStorageConnectionString,
			Usage:    "Connection string for connecting to the Azure blob storage that holds the inventory",
			Required: true,
			EnvVars:  []string{strcase.ToScreamingSnake(cliOptAzureStorageConnectionString)},
		},
		&cli.StringFlag{
			Name:    cliOptBindAddress,
			Usage:   "The TCP network address addr that is listened on.",
			Value:   ":8080",
			EnvVars: []string{strcase.ToScreamingSnake(cliOptBindAddress)},
		},
		&cli.StringFlag{
			Name:    cliOptBlobInventoryContainer,
			Usage:   "Name of the container that holds the inventory",
			Value:   "blob-inventory",
			EnvVars: []string{strcase.ToScreamingSnake(cliOptBlobInventoryContainer)},
		},
		&cli.StringFlag{
			Name:      cliOptExtraRulesFile,
			Usage:     "File to read extra rules from (they will come before the default rules)",
			EnvVars:   []string{strcase.ToScreamingSnake(cliOptExtraRulesFile)},
			TakesFile: true,
		},
	}
	defaultRules = []agg.AggregationRule{
		{Pattern: agg.NewReGroup(`^(?P<owner>)(?P<dataset>)(?P<container>argo-artifacts|container-logs|mimir-blocks|elasticsearch-snapshots)/`)},
		{Pattern: agg.NewReGroup(`^(?P<container>[^/]+)/(?P<owner>[^/]+)/(?P<dataset>[^/]+)`)},
	}
)

func main() {
	app := cli.NewApp()
	app.Name = "PDOK Storage Usage Exporter"
	app.Flags = cliFlags
	app.Action = func(c *cli.Context) error {
		aggregator, err := createAggregatorFromCliCtx(c)
		if err != nil {
			return err
		}
		metricsUpdater := serv.NewMetricsUpdater(aggregator, "pdok", "storage", 1000)

		scheduler, err := gocron.NewScheduler()
		if err != nil {
			return err
		}
		_, err = scheduler.NewJob(
			gocron.DurationJob(time.Hour), // blob inventory reports run daily or weekly, so checking hourly seems frequent enough
			gocron.NewTask(metricsUpdater.UpdatePromMetrics),
			gocron.WithName("updating metrics"),
			gocron.WithSingletonMode(gocron.LimitModeReschedule),
			gocron.WithStartAt(gocron.WithStartImmediately()),
			gocron.WithEventListeners(
				gocron.AfterJobRunsWithError(func(jobID uuid.UUID, jobName string, err error) {
					log.Printf("%s (%s) errored: %s", jobName, jobID, err.Error())
				})))
		if err != nil {
			return err
		}
		scheduler.Start()

		http.Handle("/metrics", promhttp.Handler())
		return http.ListenAndServe(c.String("bind-address"), nil)
	}

	err := app.Run(os.Args)
	if err != nil {
		log.Fatal(err)
	}
}

func createAggregatorFromCliCtx(c *cli.Context) (*agg.Aggregator, error) {
	aggregationRules, err := loadAggregationRules(c.String(cliOptExtraRulesFile))
	if err != nil {
		return nil, err
	}
	return agg.NewAggregator(
		c.String(cliOptAzureStorageConnectionString),
		c.String(cliOptBlobInventoryContainer),
		aggregationRules,
	), nil
}

func loadAggregationRules(extraRulesFile string) ([]agg.AggregationRule, error) {
	var rules []agg.AggregationRule
	if extraRulesFile != "" {
		extraRulesYaml, err := os.ReadFile(extraRulesFile)
		if err != nil {
			return nil, err
		}
		if err := yaml.Unmarshal(extraRulesYaml, &rules); err != nil {
			return nil, err
		}
	}
	rules = append(rules, defaultRules...)
	return rules, nil
}
