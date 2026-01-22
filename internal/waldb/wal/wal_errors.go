package wal

import (
	"errors"
	"fmt"
)

var (
	ErrWALClosed       = errors.New("wal: log closed")
	ErrNoSegments      = errors.New("wal: no segments")
	ErrSegmentNotFound = errors.New("wal: segment not found")
	ErrSegmentList     = errors.New("wal: list segments failed")
	ErrSegmentOpen     = errors.New("wal: open segment failed")
	ErrSegmentCreate   = errors.New("wal: create segment failed")
	ErrSegmentRotate   = errors.New("wal: rotate segment failed")
	ErrSegmentClose    = errors.New("wal: close segment failed")
	ErrSegmentFlush    = errors.New("wal: flush segment failed")
	ErrSegmentSync     = errors.New("wal: fsync segment failed")
	ErrInvalidWALDir   = errors.New("wal: invalid wal dir")
)

// LogError wraps log-level failures with context.
type LogError struct {
	Err error

	Dir   string
	SegID uint64

	// Op is a short label for where the error occurred:
	// "open", "append", "flush", "fsync", "rotate", "close", "list", etc.
	Op string

	Cause error
}

func (e *LogError) Error() string {
	if e.Op == "" {
		return e.Err.Error()
	}
	return fmt.Sprintf("%s: %s", e.Op, e.Err.Error())
}

func (e *LogError) Unwrap() error {
	return e.Err
}

func (e *LogError) CauseErr() error { return e.Cause }
