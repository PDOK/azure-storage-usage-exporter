package main

import (
	"log"
	"net/http"
	"os"
	"time"

	"github.com/PDOK/azure-storage-usage-exporter/internal/du"

	"github.com/google/uuid"

	"github.com/PDOK/azure-storage-usage-exporter/internal/metrics"
	"github.com/go-co-op/gocron/v2"
	"github.com/iancoleman/strcase"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/PDOK/azure-storage-usage-exporter/internal/agg"

	"gopkg.in/yaml.v2"

	"github.com/urfave/cli/v2"
)

type Config struct {
	Labels agg.Labels            `yaml:"labels"`
	Rules  []agg.AggregationRule `yaml:"rules"`
}

const (
	cliOptAzureStorageConnectionString = "azure-storage-connection-string"
	cliOptBindAddress                  = "bind-address"
	cliOptBlobInventoryContainer       = "blob-inventory-container"
	cliOptConfigFile                   = "config"
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
			Name:      cliOptConfigFile,
			Usage:     "Config file with aggregation labels and rules",
			EnvVars:   []string{strcase.ToScreamingSnake(cliOptConfigFile)},
			Required:  true,
			TakesFile: true,
		},
	}
)

func main() {
	app := cli.NewApp()
	app.HelpName = "Azure Storage Usage Exporter"
	app.Name = "azure-storage-usage-exporter"
	app.Usage = "Aggregates an Azure Blob Inventory Report and export as Prometheus metrics"
	app.Flags = cliFlags
	app.Action = func(c *cli.Context) error {
		aggregator, err := createAggregatorFromCliCtx(c)
		if err != nil {
			return err
		}
		metricsUpdater := metrics.NewUpdater(aggregator, "pdok", "storage", 1000)

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
		server := &http.Server{
			Addr:              c.String("bind-address"),
			ReadHeaderTimeout: 10 * time.Second,
		}
		return server.ListenAndServe()
	}

	err := app.Run(os.Args)
	if err != nil {
		log.Fatal(err)
	}
}

func createAggregatorFromCliCtx(c *cli.Context) (*agg.Aggregator, error) {
	config, err := loadConfig(c.String(cliOptConfigFile))
	if err != nil {
		return nil, err
	}
	duReader := du.NewAzureBlobInventoryReportDuReader(c.String(cliOptAzureStorageConnectionString),
		c.String(cliOptBlobInventoryContainer))
	if err := duReader.TestConnection(); err != nil {
		return nil, err
	}
	return agg.NewAggregator(
		duReader,
		config.Labels,
		config.Rules,
	)
}

func loadConfig(configFile string) (*Config, error) {
	config := new(Config)
	configYaml, err := os.ReadFile(configFile)
	if err != nil {
		return nil, err
	}
	if err := yaml.Unmarshal(configYaml, &config); err != nil {
		return nil, err
	}
	return config, nil
}
