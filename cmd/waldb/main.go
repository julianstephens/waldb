package main

import (
	"errors"
	"fmt"
	"os"

	"github.com/alecthomas/kong"
	"github.com/julianstephens/waldb/internal/cli"
)

var (
	version = "dev"
	commit  = "unknown"
	date    = "unknown"
)

// CLI defines the command-line interface structure.
type CLI struct {
	Init     cli.InitCmd     `cmd:"" help:"Initialize a new WAL database"`
	Get      cli.GetCmd      `cmd:"" help:"Get a value by key"`
	Put      cli.PutCmd      `cmd:"" help:"Put a key-value pair"`
	Del      cli.DelCmd      `cmd:"" help:"Delete a key"`
	Batch    cli.BatchCmd    `cmd:"" help:"Execute multiple operations in a batch"`
	Snapshot cli.SnapshotCmd `cmd:"" help:"Create a database snapshot"`
	Stats    cli.StatsCmd    `cmd:"" help:"Display database statistics"`
	Doctor   cli.DoctorCmd   `cmd:"" help:"Check database health and integrity"`
	Repair   cli.RepairCmd   `cmd:"" help:"Repair a corrupted database"`

	Version VersionFlag `name:"version" help:"Show version information" short:"v"`
}

// VersionFlag is a custom flag for displaying version information.
type VersionFlag bool

func (v VersionFlag) BeforeApply(ctx *kong.Context) error {
	fmt.Printf("waldb %s (commit: %s, built: %s)\n", version, commit, date)
	ctx.Kong.Exit(0)
	return nil
}

func main() {
	cliApp := &CLI{}
	ctx := kong.Parse(cliApp,
		kong.Name("waldb"),
		kong.Description("A Write-Ahead Log database"),
		kong.UsageOnError(),
		kong.ConfigureHelp(kong.HelpOptions{
			Compact: true,
		}),
		kong.Vars{
			"version": version,
		},
	)

	err := ctx.Run()
	if err != nil {
		if errors.Is(err, cli.ErrNotImplemented) {
			os.Exit(2)
		}
		ctx.FatalIfErrorf(err)
	}
}
