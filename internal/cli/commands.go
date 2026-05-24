package cli

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"

	"github.com/julianstephens/go-utils/cliutil"
	"github.com/julianstephens/go-utils/generic"

	"github.com/julianstephens/waldb/internal/logger"
	"github.com/julianstephens/waldb/internal/waldb"
	"github.com/julianstephens/waldb/internal/waldb/db"
	"github.com/julianstephens/waldb/internal/waldb/manifest"
)

// ErrNotImplemented is returned when a command is not yet implemented.
var ErrNotImplemented = errors.New("not yet implemented")

type Globals struct {
	DB string `help:"Path to the database" env:"WALDB_DB_PATH" short:"d" required:""`
}

// InitCmd initializes a new WAL database.
type InitCmd struct{}

func (c *InitCmd) Run(globals Globals, lg logger.Logger) error {
	cliutil.PrintInfo(fmt.Sprintf("Initializing database at %s", globals.DB))
	if err := waldb.Init(globals.DB, lg); err != nil {
		return err
	}
	cliutil.PrintSuccess("Database initialized successfully")
	return nil
}

type OutputOptions struct {
	Json   bool `help:"Output in JSON format" default:"false"`
	Pretty bool `help:"Pretty-print output"   default:"false" env:"WALDB_PRETTY_PRINT"`
}

// GetCmd retrieves a value by key.
type GetCmd struct {
	Key string `arg:"" help:"Key to retrieve"`
	OutputOptions
}

func (c *GetCmd) Run(globals Globals, lg logger.Logger) (err error) {
	d, err := db.Open(globals.DB, lg)
	if err != nil {
		return
	}
	defer closeDB(d, &err)

	val, err := d.Get([]byte(c.Key))
	if err != nil {
		return
	}
	if err = printDBEntry(c.Key, string(val), c.Json, c.Pretty); err != nil {
		return
	}
	return
}

// PutCmd stores a key-value pair.
type PutCmd struct {
	Key   string `arg:"" help:"Key to store"`
	Value string `arg:"" help:"Value to store"`
	OutputOptions
}

func (c *PutCmd) Run(globals Globals, lg logger.Logger) (err error) {
	d, err := db.Open(globals.DB, lg)
	if err != nil {
		return
	}
	defer closeDB(d, &err)

	if err = d.Put([]byte(c.Key), []byte(c.Value)); err != nil {
		return
	}
	if err = printDBEntry(c.Key, c.Value, c.Json, c.Pretty); err != nil {
		return
	}
	return
}

// DelCmd deletes a key.
type DelCmd struct {
	Key string `arg:"" help:"Key to delete"`
	OutputOptions
}

func (c *DelCmd) Run(globals Globals, lg logger.Logger) (err error) {
	d, err := db.Open(globals.DB, lg)
	if err != nil {
		return
	}
	defer closeDB(d, &err)

	if err = d.Delete([]byte(c.Key)); err != nil {
		return
	}
	if err = printDBEntry(c.Key, "<deleted>", c.Json, c.Pretty); err != nil {
		return
	}
	return
}

// BatchCmd executes multiple operations in a batch.
type BatchCmd struct {
	File string `arg:"" help:"File containing batch operations"`
}

func (c *BatchCmd) Run(globals Globals, lg logger.Logger) error {
	return ErrNotImplemented
}

// SnapshotCmd creates a database snapshot.
type SnapshotCmd struct{}

func (c *SnapshotCmd) Run(globals Globals, lg logger.Logger) error {
	return ErrNotImplemented
}

// StatsCmd displays database statistics.
type StatsCmd struct{}

func (c *StatsCmd) Run(globals Globals, lg logger.Logger) error {
	return ErrNotImplemented
}

// DoctorCmd checks database health and integrity.
type DoctorCmd struct{}

func (c *DoctorCmd) Run(globals Globals, lg logger.Logger) error {
	return ErrNotImplemented
}

// RepairCmd repairs a corrupted database.
type RepairCmd struct{}

func (c *RepairCmd) Run(globals Globals, lg logger.Logger) error {
	return ErrNotImplemented
}

// ManifestCmd displays the database manifest.
type ManifestCmd struct {
	OutputOptions
}

func (c *ManifestCmd) Run(globals Globals, lg logger.Logger) error {
	m, err := manifest.Open(globals.DB)
	if err != nil {
		return err
	}
	if m == nil {
		return errors.New("manifest not found")
	}

	if c.Json {
		return printJson(m, c.Pretty)
	} else {
		if c.Pretty {
			cliutil.PrintColored("Database Manifest:", cliutil.ColorCyan)
			cliutil.PrintColored("-------------------", cliutil.ColorCyan)
			defer cliutil.PrintColored("-------------------", cliutil.ColorCyan)
			manifestMap := m.ToMap()
			manifestMapKeys := generic.Keys(manifestMap)
			sort.Strings(manifestMapKeys)

			for i, key := range manifestMapKeys {
				value := manifestMap[key]
				cliutil.PrintColored(
					fmt.Sprintf("%s: %v", key, value),
					generic.If(i%2 == 0, cliutil.ColorYellow, cliutil.ColorGreen),
				)
			}
		} else {
			for key, value := range m.ToMap() {
				fmt.Printf("%s: %v\n", key, value)
			}
		}
	}
	return nil
}

func printDBEntry(key, value string, doJson, pretty bool) error {
	if doJson {
		entry := map[string]string{"key": key, "value": value}
		if err := printJson(entry, pretty); err != nil {
			return err
		}
	} else {
		if pretty {
			cliutil.PrintColored("-------------------", cliutil.ColorCyan)
			defer cliutil.PrintColored("-------------------", cliutil.ColorCyan)
			cliutil.PrintColored(fmt.Sprintf("Key: %s", key), cliutil.ColorYellow)
			cliutil.PrintColored(fmt.Sprintf("Value: %s", value), cliutil.ColorGreen)
		} else {
			cliutil.PrintColored(fmt.Sprintf("Key: %s\nValue: %s", key, value), cliutil.ColorGreen)
		}
	}
	return nil
}

func printJson(data any, pretty bool) error {
	if pretty {
		b, err := json.MarshalIndent(data, "", "  ")
		if err != nil {
			fmt.Printf("Error formatting JSON: %v\n", err)
			return err
		}
		fmt.Println(string(b))
	} else {
		b, err := json.Marshal(data)
		if err != nil {
			fmt.Printf("Error formatting JSON: %v\n", err)
			return err
		}
		fmt.Println(string(b))
	}
	return nil
}

func closeDB(db *db.DB, errp *error) {
	if closeErr := db.Close(); closeErr != nil && *errp == nil {
		*errp = closeErr
	}
}
