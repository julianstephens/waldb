package main

import (
	"errors"
	"fmt"
	"os"

	"github.com/alecthomas/kong"

	"github.com/julianstephens/go-utils/cliutil"
	"github.com/julianstephens/waldb/internal/cli"
	"github.com/julianstephens/waldb/internal/logger"
)

var (
	version = "dev"
	commit  = "abb3796"
	date    = "2026-01-18T19:39"
)

type LogOpts struct {
	Level  string `help:"Logging level (debug, info, warn, error)" default:"info" envvar:"WALDB_LOG_LEVEL"`
	Debug  bool   `help:"Enable debug logging (overrides --level)"                envvar:"WALDB_DEBUG"`
	Stream bool   `help:"Log to stdout/stderr in addition to file"                envvar:"WALDB_LOG_STREAM"`
}

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

	Logger  logger.Logger `kong:"-"` // Internal logger, not exposed as CLI flag
	LogOpts LogOpts       `         embed:"" prefix:"log-" help:"Logging options"`
	Version VersionFlag   `                                help:"Show version information" name:"version" short:"v"`
}

// VersionFlag is a custom flag for displaying version information.
type VersionFlag bool

func (v VersionFlag) BeforeApply(ctx *kong.Context) error {
	cliutil.PrintInfo(fmt.Sprintf("waldb %s (commit: %s, built: %s)\n", version, commit, date))
	ctx.Exit(0)
	return nil
}

func createLogger(opts LogOpts) (logger.Logger, error) {
	var level string
	if opts.Debug {
		level = "debug"
	} else {
		level = opts.Level
	}

	consoleLogger := logger.NewConsoleLogger(level)

	if opts.Stream {
		return consoleLogger, nil
	}

	// FIXME: should use manifest values
	logDir := "logs"
	logFileName := "waldb.log"
	fileLogger, err := logger.NewFileLogger(logDir, logFileName, 10, 5)
	if err != nil {
		return nil, err
	}

	multiLogger := logger.NewMultiLogger(fileLogger, consoleLogger)
	return multiLogger, nil
}

func main() {
	cliApp := &CLI{
		Logger: logger.NoOpLogger{}, // Default to no-op logger
	}
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

	// Create logger from CLI options
	lg, err := createLogger(cliApp.LogOpts)
	if err != nil {
		ctx.FatalIfErrorf(err)
	}
	cliApp.Logger = lg

	// Ensure logger is properly closed
	defer func() {
		if c, ok := lg.(logger.Closeable); ok {
			_ = c.Close()
		}
	}()

	err = ctx.Run()
	if err != nil {
		if errors.Is(err, cli.ErrNotImplemented) {
			os.Exit(2)
		}
		ctx.FatalIfErrorf(err)
	}
}
