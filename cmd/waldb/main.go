package main

import (
	"errors"
	"fmt"
	"os"
	"path"

	"github.com/alecthomas/kong"
	"github.com/julianstephens/go-utils/cliutil"

	"github.com/julianstephens/waldb/internal/cli"
	"github.com/julianstephens/waldb/internal/logger"
	"github.com/julianstephens/waldb/internal/waldb/config"
	"github.com/julianstephens/waldb/internal/waldb/manifest"
)

type LogOpts struct {
	Level       string `help:"Logging level (debug, info, warn, error)"           default:"warn" envvar:"WALDB_LOG_LEVEL"`
	Debug       bool   `help:"Enable debug logging (overrides --level)"                          envvar:"WALDB_DEBUG"`
	ConsoleOnly bool   `help:"Log only to console, not to file"                                  envvar:"WALDB_CONSOLE_ONLY"`
	Quiet       bool   `help:"Disable console logging (file logger still active)"                envvar:"WALDB_QUIET"`
}
type CLI struct {
	cli.Globals

	Init     cli.InitCmd     `cmd:"" help:"Initialize a new WAL database"`
	Get      cli.GetCmd      `cmd:"" help:"Get a value by key"`
	Put      cli.PutCmd      `cmd:"" help:"Put a key-value pair"`
	Del      cli.DelCmd      `cmd:"" help:"Delete a key"`
	Batch    cli.BatchCmd    `cmd:"" help:"Execute multiple operations in a batch"`
	Snapshot cli.SnapshotCmd `cmd:"" help:"Create a database snapshot"`
	Stats    cli.StatsCmd    `cmd:"" help:"Display database statistics"`
	Doctor   cli.DoctorCmd   `cmd:"" help:"Check database health and integrity"`
	Repair   cli.RepairCmd   `cmd:"" help:"Repair a corrupted database"`
	Manifest cli.ManifestCmd `cmd:"" help:"Display manifest information"`

	// Internal logger, not exposed as CLI flag
	Logger logger.Logger `kong:"-"`
	// nolint:golines // keep struct field aligned
	LogOpts LogOpts     `embed:"" prefix:"log-" help:"Logging options"`
	Version VersionFlag `                       help:"Show version information" short:"V"`
}

type VersionFlag string

func (v VersionFlag) Decode(ctx *kong.DecodeContext) error { return nil }
func (v VersionFlag) IsBool() bool                         { return true }
func (v VersionFlag) BeforeApply(app *kong.Kong, vars kong.Vars) error {
	cliutil.PrintColored(fmt.Sprintf("waldb v%s", vars["version"]), cliutil.ColorCyan)
	app.Exit(0)
	return nil
}

func createLogger(opts LogOpts, m *manifest.Manifest) (logger.Logger, error) {
	var level string
	if opts.Debug {
		level = "debug"
	} else {
		level = opts.Level
	}
	if opts.Quiet {
		level = "error" // Only log errors to console when quiet mode is enabled
	}

	consoleLogger := logger.NewConsoleLogger(level)
	if opts.ConsoleOnly {
		return consoleLogger, nil
	}

	var logDir, logFileName string
	var logMaxSize, logMaxBackups int
	if m != nil {
		logDir = m.LogDirOrDefault()
		logFileName = m.LogFileNameOrDefault()
		logMaxSize = m.LogMaxSizeOrDefault()
		logMaxBackups = m.LogMaxBackupsOrDefault()
	} else {
		wd, err := os.Getwd()
		if err != nil {
			return nil, err
		}
		logDir = path.Join(wd, config.DefaultLogDir)
		logFileName = config.DefaultLogFileName
		logMaxSize = config.DefaultLogMaxSize
		logMaxBackups = config.DefaultLogMaxBackups
	}

	fileLogger, err := logger.NewFileLogger(logDir, logFileName, logMaxSize, logMaxBackups)
	if err != nil {
		return nil, err
	}

	multiLogger := logger.NewMultiLogger(fileLogger, consoleLogger)
	return multiLogger, nil
}

func main() {
	cliApp := &CLI{
		Globals: cli.Globals{},
		Logger:  logger.NoOpLogger{}, // Default to no-op logger
	}
	ctx := kong.Parse(cliApp,
		kong.Name("waldb"),
		kong.Description("A Write-Ahead Log database"),
		kong.UsageOnError(),
		kong.ConfigureHelp(kong.HelpOptions{
			Compact: true,
		}),
		kong.Vars{
			"version": config.Version,
		},
	)

	// Create logger from CLI options.
	// If --db points to an existing DB, read log config from its manifest so
	// the file logger uses the paths recorded at init time. Fall back to
	// defaults when the manifest is absent (e.g. before init runs).
	m, _ := manifest.Open(cliApp.Globals.DB)
	lg, err := createLogger(cliApp.LogOpts, m)
	if err != nil {
		ctx.FatalIfErrorf(err)
	}
	ctx.Bind(lg)
	ctx.BindTo(lg, (*logger.Logger)(nil))

	// Ensure logger is properly closed
	defer func() {
		if c, ok := lg.(logger.Closeable); ok {
			_ = c.Close()
		}
	}()

	err = ctx.Run(cliApp.Globals)
	if err != nil {
		if errors.Is(err, cli.ErrNotImplemented) {
			cliutil.PrintError("Error: Command not yet implemented")
			os.Exit(2)
		}
		cliutil.PrintError(fmt.Sprintf("Error: %v", err))
		os.Exit(1)
	}
}
