package wal

import (
	"io"

	"github.com/julianstephens/waldb/internal/waldb/wal/record"
)

const (
	FirstSegmentID uint64 = 1
)

type LogAppender interface {
	// Append appends a record of the given type with the provided payload to the log.
	// It returns the offset of the start of the record header in the segment.
	Append(recordType record.RecordType, payload []byte) (offset int64, err error)
	Flush() error
	FSync() error
	Close() error
}

type SegmentProvider interface {
	// SegmentIDs returns WAL segment IDs in ascending order.
	SegmentIDs() []uint64
	// OpenSegment opens a reader for the given segment id.
	OpenSegment(segID uint64) (SegmentReader, error)
}

type SegmentReader interface {
	SegID() uint64
	SeekTo(offset int64) error // seek to absolute byte offset within the segment
	Reader() io.Reader         // stream starting at current position
	Close() error
}
