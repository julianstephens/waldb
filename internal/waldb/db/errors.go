package db

import (
	"errors"
	"fmt"
)

var (
	ErrInvalidPath        = errors.New("db: invalid path")
	ErrOpenFailed         = errors.New("db: open failed")
	ErrInitFailed         = errors.New("db: init failed")
	ErrClosed             = errors.New("db: closed")
	ErrReplayFailed       = errors.New("db: replay failed")
	ErrWALOpenFailed      = errors.New("db: wal open failed")
	ErrCommitFailed       = errors.New("db: commit failed")
	ErrCommitInvalidBatch = errors.New("db: commit invalid batch")
)

// DBError wraps DB-layer failures with stable sentinels for errors.Is,
// while preserving Cause for inspection/logging.
type DBError struct {
	Err error

	// Op describes the operation: "open", "init", "close", "get", "put", etc.
	Op string

	// Path is the db path (wal dir).
	Path string

	Cause error
}

func (e *DBError) Error() string {
	if e.Op == "" {
		return e.Err.Error()
	}
	if e.Path != "" {
		return fmt.Sprintf("%s: %s (%s)", e.Op, e.Err.Error(), e.Path)
	}
	return fmt.Sprintf("%s: %s", e.Op, e.Err.Error())
}

func (e *DBError) Unwrap() error { return e.Err }

func (e *DBError) CauseErr() error { return e.Cause }

func wrapDBErr(op string, sentinel error, path string, cause error) error {
	return &DBError{
		Err:   sentinel,
		Op:    op,
		Path:  path,
		Cause: cause,
	}
}
