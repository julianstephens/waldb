package cli

import (
	"fmt"
	"os"
)

const exitNotImplemented = 2

// InitCmd initializes a new WAL database.
type InitCmd struct {
	Path string `arg:"" help:"Path to the database"`
}

func (c *InitCmd) Run() error {
	fmt.Fprintf(os.Stderr, "Command 'init' is not yet implemented\n")
	os.Exit(exitNotImplemented)
	return nil
}

// GetCmd retrieves a value by key.
type GetCmd struct {
	Key string `arg:"" help:"Key to retrieve"`
}

func (c *GetCmd) Run() error {
	fmt.Fprintf(os.Stderr, "Command 'get' is not yet implemented\n")
	os.Exit(exitNotImplemented)
	return nil
}

// PutCmd stores a key-value pair.
type PutCmd struct {
	Key   string `arg:"" help:"Key to store"`
	Value string `arg:"" help:"Value to store"`
}

func (c *PutCmd) Run() error {
	fmt.Fprintf(os.Stderr, "Command 'put' is not yet implemented\n")
	os.Exit(exitNotImplemented)
	return nil
}

// DelCmd deletes a key.
type DelCmd struct {
	Key string `arg:"" help:"Key to delete"`
}

func (c *DelCmd) Run() error {
	fmt.Fprintf(os.Stderr, "Command 'del' is not yet implemented\n")
	os.Exit(exitNotImplemented)
	return nil
}

// BatchCmd executes multiple operations in a batch.
type BatchCmd struct {
	File string `arg:"" help:"File containing batch operations"`
}

func (c *BatchCmd) Run() error {
	fmt.Fprintf(os.Stderr, "Command 'batch' is not yet implemented\n")
	os.Exit(exitNotImplemented)
	return nil
}

// SnapshotCmd creates a database snapshot.
type SnapshotCmd struct {
	Path string `arg:"" help:"Path for the snapshot"`
}

func (c *SnapshotCmd) Run() error {
	fmt.Fprintf(os.Stderr, "Command 'snapshot' is not yet implemented\n")
	os.Exit(exitNotImplemented)
	return nil
}

// StatsCmd displays database statistics.
type StatsCmd struct{}

func (c *StatsCmd) Run() error {
	fmt.Fprintf(os.Stderr, "Command 'stats' is not yet implemented\n")
	os.Exit(exitNotImplemented)
	return nil
}

// DoctorCmd checks database health and integrity.
type DoctorCmd struct{}

func (c *DoctorCmd) Run() error {
	fmt.Fprintf(os.Stderr, "Command 'doctor' is not yet implemented\n")
	os.Exit(exitNotImplemented)
	return nil
}

// RepairCmd repairs a corrupted database.
type RepairCmd struct {
	Path string `arg:"" help:"Path to the database to repair"`
}

func (c *RepairCmd) Run() error {
	fmt.Fprintf(os.Stderr, "Command 'repair' is not yet implemented\n")
	os.Exit(exitNotImplemented)
	return nil
}
