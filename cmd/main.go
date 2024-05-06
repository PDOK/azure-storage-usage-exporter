package main

import (
	"log"
	"os"

	"github.com/urfave/cli/v2"
)

var (
	cliFlags = []cli.Flag{}
)

func main() {
	app := cli.NewApp()
	app.Name = "PDOK Storage Usage Explorer"
	app.Flags = cliFlags
	app.Action = func(_ *cli.Context) error {
		log.Println("hello world")
		return nil
	}

	err := app.Run(os.Args)
	if err != nil {
		log.Fatal(err)
	}
}
