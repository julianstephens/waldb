package wal

import (
	"bufio"
	"io"
	"os"

	"github.com/julianstephens/waldb/internal/waldb/wal/record"
)

const (
	segmentWriterBufferSize = 64 << 10 // 64KiB
)

type SegmentWriter struct {
	currSegmentFile *os.File
	currOffset      int64
	writer          *bufio.Writer
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
	}, nil
}

func (sw *SegmentWriter) Append(recordType record.RecordType, payload []byte) (offset int64, err error) {
	if err = record.ValidateRecord(recordType, payload); err != nil {
		err = &SegmentWriteError{
			Err:        ErrInvalidRecord,
			Offset:     sw.currOffset,
			RecordType: recordType,
		}
		return
	}

	framedRecord, err := record.Encode(recordType, payload)
	if err != nil {
		err = &SegmentWriteError{
			Err:        ErrInvalidRecord,
			Offset:     sw.currOffset,
			RecordType: recordType,
		}
		return
	}
	if len(framedRecord) > record.MaxRecordSize {
		err = &SegmentWriteError{
			Err:        ErrInvalidRecord,
			Offset:     sw.currOffset,
			RecordType: recordType,
			Have:       len(framedRecord),
			Want:       record.MaxRecordSize,
		}
		return
	}

	offset = sw.currOffset

	n, err := sw.writer.Write(framedRecord)
	if err != nil {
		err = &SegmentWriteError{
			Err:        ErrAppendFailed,
			Offset:     sw.currOffset,
			RecordType: recordType,
			Have:       n,
			Want:       len(framedRecord),
		}
		return
	}
	sw.currOffset += int64(n)

	if n < len(framedRecord) {
		err = &SegmentWriteError{
			Err:        ErrShortWrite,
			Offset:     sw.currOffset,
			RecordType: recordType,
			Have:       n,
			Want:       len(framedRecord),
		}
		return
	}

	return
}

func (sw *SegmentWriter) Flush() error {
	return nil
}

func (sw *SegmentWriter) FSync() error {
	return nil
}

func (sw *SegmentWriter) Close() error {
	return nil
}