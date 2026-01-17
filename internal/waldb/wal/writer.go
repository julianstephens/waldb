package wal

import "github.com/julianstephens/waldb/internal/waldb/wal/record"

type LogWriter interface {
	// Append appends a record of the given type with the provided payload to the log.
	// It returns the offset of the start of the record header in the segment.
	Append(recordType record.RecordType, payload []byte) (offset int64, err error)
	Flush() error
	FSync() error
	Close() error
}
