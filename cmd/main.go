package main

import (
	"fmt"
	"log"
	"os"

	"github.com/PDOK/azure-storage-usage-exporter/internal/agg"

	"gopkg.in/yaml.v2"

	"github.com/urfave/cli/v2"
)

var (
	cliFlags = []cli.Flag{
		&cli.StringFlag{
			Name:     "azure-storage-connection-string",
			Usage:    "Connection string for connecting to the Azure blob storage that holds the inventory",
			Required: true,
			EnvVars:  []string{"AZURE_STORAGE_CONNECTION_STRING"},
		},
		&cli.StringFlag{
			Name:    "blob-inventory-container",
			Usage:   "Name of the container that holds the inventory",
			Value:   "blob-inventory",
			EnvVars: []string{"BLOB_INVENTORY_CONTAINER"},
		},
		&cli.StringFlag{
			Name:      "extra-rules-file",
			Usage:     "File to read extra rules from (they will come before the default rules)",
			EnvVars:   []string{"EXTRA_RULES"},
			TakesFile: true,
		},
	}
	defaultRules = []agg.AggregationRule{
		{Pattern: agg.NewReGroup(`^(?P<owner>)(?P<dataset>)(?P<container>argo-artifacts)/`)},
		{Pattern: agg.NewReGroup(`^(?P<owner>)(?P<dataset>)(?P<container>container-logs)/`)},
		{Pattern: agg.NewReGroup(`^(?P<container>[^/]+)/(?P<owner>[^/]+)/(?P<dataset>[^/]+)`)},
	}
)

func main() {
	app := cli.NewApp()
	app.Name = "PDOK Storage Usage Exporter"
	app.Flags = cliFlags
	app.Action = func(c *cli.Context) error {
		aggregationRules, err := gatherAggregationRules(c.String("extra-rules-file"))
		if err != nil {
			return err
		}
		aggregator := agg.Aggregator{
			AzureStorageConnectionString: c.String("azure-storage-connection-string"),
			BlobInventoryContainer:       c.String("blob-inventory-container"),
			Rules:                        aggregationRules,
		}
		aggregationResult, err := aggregator.Aggregate()
		if err != nil {
			return err
		}
		fmt.Println(aggregationResult)
		return nil
	}

	err := app.Run(os.Args)
	if err != nil {
		log.Fatal(err)
	}
}

func gatherAggregationRules(extraRulesFile string) ([]agg.AggregationRule, error) {
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
