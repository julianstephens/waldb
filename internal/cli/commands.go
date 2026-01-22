package cli

import (
	"errors"

	"github.com/julianstephens/go-utils/cliutil"
)

// ErrNotImplemented is returned when a command is not yet implemented.
var ErrNotImplemented = errors.New("not yet implemented")

// InitCmd initializes a new WAL database.
type InitCmd struct {
	Path string `arg:"" help:"Path to the database"`
}

func (c *InitCmd) Run() error {
	cliutil.PrintError("Command 'init' is not yet implemented")
	return ErrNotImplemented
}

// GetCmd retrieves a value by key.
type GetCmd struct {
	Key string `arg:"" help:"Key to retrieve"`
}

func (c *GetCmd) Run() error {
	cliutil.PrintError("Command 'get' is not yet implemented")
	return ErrNotImplemented
}

// PutCmd stores a key-value pair.
type PutCmd struct {
	Key   string `arg:"" help:"Key to store"`
	Value string `arg:"" help:"Value to store"`
}

func (c *PutCmd) Run() error {
	cliutil.PrintError("Command 'put' is not yet implemented")
	return ErrNotImplemented
}

// DelCmd deletes a key.
type DelCmd struct {
	Key string `arg:"" help:"Key to delete"`
}

func (c *DelCmd) Run() error {
	cliutil.PrintError("Command 'del' is not yet implemented")
	return ErrNotImplemented
}

// BatchCmd executes multiple operations in a batch.
type BatchCmd struct {
	File string `arg:"" help:"File containing batch operations"`
}

func (c *BatchCmd) Run() error {
	cliutil.PrintError("Command 'batch' is not yet implemented")
	return ErrNotImplemented
}

// SnapshotCmd creates a database snapshot.
type SnapshotCmd struct {
	Path string `arg:"" help:"Path for the snapshot"`
}

func (c *SnapshotCmd) Run() error {
	cliutil.PrintError("Command 'snapshot' is not yet implemented")
	return ErrNotImplemented
}

// StatsCmd displays database statistics.
type StatsCmd struct{}

func (c *StatsCmd) Run() error {
	cliutil.PrintError("Command 'stats' is not yet implemented")
	return ErrNotImplemented
}

// DoctorCmd checks database health and integrity.
type DoctorCmd struct{}

func (c *DoctorCmd) Run() error {
	cliutil.PrintError("Command 'doctor' is not yet implemented")
	return ErrNotImplemented
}

// RepairCmd repairs a corrupted database.
type RepairCmd struct {
	Path string `arg:"" help:"Path to the database to repair"`
}

func (c *RepairCmd) Run() error {
	cliutil.PrintError("Command 'repair' is not yet implemented")
	return ErrNotImplemented
}
