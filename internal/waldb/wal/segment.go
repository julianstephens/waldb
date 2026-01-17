package wal

import (
	"bufio"
	"io"
	"os"
	"sync"

	"github.com/julianstephens/waldb/internal/waldb/wal/record"
)

const (
	segmentWriterBufferSize = 64 << 10 // 64KiB
)

// SegmentWriter appends records to a WAL segment file.
// It is safe for concurrent use by multiple goroutines, but
// all operations are serialized internally. Append, Flush,
// FSync, and Close must not be long-running
type SegmentWriter struct {
	currSegmentFile *os.File
	currOffset      int64
	writer          *bufio.Writer
	closed          bool
	mu              sync.Mutex
}

// NewSegmentWriter creates a new SegmentWriter that appends records to the given segment file.
func NewSegmentWriter(segmentFile *os.File) (*SegmentWriter, error) {
	if segmentFile == nil {
		return nil, ErrNilSegmentFile
	}

	info, err := segmentFile.Stat()
	if err != nil {
		return nil, err
	}
	size := info.Size()

	if _, err := segmentFile.Seek(0, io.SeekEnd); err != nil {
		return nil, err
	}

	return &SegmentWriter{
		currSegmentFile: segmentFile,
		currOffset:      size,
		writer:          bufio.NewWriterSize(segmentFile, segmentWriterBufferSize),
		closed:          false,
		mu:              sync.Mutex{},
	}, nil
}

// Append appends a record with the given type and payload to the segment.
// It returns the offset where the record was written or an error.
func (sw *SegmentWriter) Append(recordType record.RecordType, payload []byte) (offset int64, err error) {
	sw.mu.Lock()
	defer sw.mu.Unlock()

	if sw.closed {
		err = &SegmentWriteError{
			Err:    ErrClosedWriter,
			Offset: sw.currOffset,
		}
		return
	}
	if err2 := record.ValidateRecordFrame(recordType, payload); err2 != nil {
		err = &SegmentWriteError{
			Err:        ErrInvalidRecord,
			Cause:      err2,
			Offset:     sw.currOffset,
			RecordType: recordType,
		}
		return
	}

	framedRecord, err2 := record.Encode(recordType, payload)
	if err2 != nil {
		err = &SegmentWriteError{
			Err:        ErrInvalidRecord,
			Cause:      err2,
			Offset:     sw.currOffset,
			RecordType: recordType,
		}
		return
	}
	offset = sw.currOffset

	n, err2 := sw.writer.Write(framedRecord)
	if err2 != nil {
		err = &SegmentWriteError{
			Err:        ErrAppendFailed,
			Cause:      err2,
			Offset:     sw.currOffset,
			RecordType: recordType,
			Have:       n,
			Want:       len(framedRecord),
		}
		return
	}

	if n < len(framedRecord) {
		err = &SegmentWriteError{
			Err:        ErrShortWrite,
			Cause:      io.ErrShortWrite,
			Offset:     sw.currOffset,
			RecordType: recordType,
			Have:       n,
			Want:       len(framedRecord),
		}
		return
	}

	sw.currOffset += int64(len(framedRecord))

	return
}

// Flush flushes any buffered data to the underlying segment file.
func (sw *SegmentWriter) Flush() error {
	sw.mu.Lock()
	defer sw.mu.Unlock()

	if sw.closed {
		return &SegmentWriteError{
			Err:    ErrClosedWriter,
			Offset: sw.currOffset,
		}
	}

	if err := sw.flush(); err != nil {
		return err
	}

	return nil
}

// FSync flushes any buffered data and syncs the underlying segment file to disk.
func (sw *SegmentWriter) FSync() error {
	sw.mu.Lock()
	defer sw.mu.Unlock()

	if sw.closed {
		return &SegmentWriteError{
			Err:    ErrClosedWriter,
			Offset: sw.currOffset,
		}
	}

	if err := sw.flush(); err != nil {
		return err
	}

	if err := sw.currSegmentFile.Sync(); err != nil {
		return &SegmentWriteError{
			Err:    ErrSyncFailed,
			Cause:  err,
			Offset: sw.currOffset,
		}
	}

	return nil
}

// Close flushes any buffered data and closes the underlying segment file.
// After calling Close, the SegmentWriter cannot be used again.
func (sw *SegmentWriter) Close() error {
	sw.mu.Lock()
	defer sw.mu.Unlock()

	if sw.closed {
		return nil
	}
	sw.closed = true

	if err := sw.flush(); err != nil {
		return err
	}

	if err := sw.currSegmentFile.Close(); err != nil {
		return &SegmentWriteError{
			Err:    ErrCloseFailed,
			Cause:  err,
			Offset: sw.currOffset,
		}
	}

	sw.writer = nil
	sw.currSegmentFile = nil

	return nil
}

func (sw *SegmentWriter) flush() error {
	if err := sw.writer.Flush(); err != nil {
		return &SegmentWriteError{
			Err:    ErrFlushFailed,
			Cause:  err,
			Offset: sw.currOffset,
		}
	}
	return nil
}
