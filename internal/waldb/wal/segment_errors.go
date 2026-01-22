package wal

import (
	"errors"

	"github.com/julianstephens/waldb/internal/waldb/wal/record"
)

var (
	// Programmer / caller error
	ErrInvalidRecord = errors.New("wal: invalid record")

	// I/O layer failures
	ErrAppendFailed = errors.New("wal: append failed")
	ErrShortWrite   = errors.New("wal: short write")
	ErrFlushFailed  = errors.New("wal: flush failed")
	ErrSyncFailed   = errors.New("wal: fsync failed")
	ErrCloseFailed  = errors.New("wal: close failed")

	// Construction / lifecycle errors
	ErrNilSegmentFile = errors.New("wal: nil segment file")
	ErrClosedWriter   = errors.New("wal: segment writer closed")
)

type SegmentAppendError struct {
	Err        error
	Cause      error // underlying error, if any
	Offset     int64 // offset where write was attempted
	RecordType record.RecordType
	Have       int // bytes written (if short write)
	Want       int // bytes expected
}

func (e *SegmentAppendError) Error() string { return e.Err.Error() }
func (e *SegmentAppendError) Unwrap() error { return e.Err }
