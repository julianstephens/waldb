package wal

import (
	"io"
	"os"
)

type SegmentReader interface {
	SegID() uint64
	SeekTo(offset int64) error // seek to absolute byte offset within the segment
	Reader() io.Reader         // stream starting at current position
	Close() error
}

type SegmentProvider interface {
	// SegmentIDs returns WAL segment IDs in ascending order.
	SegmentIDs() []uint64
	// OpenSegment opens a reader for the given segment id.
	OpenSegment(segID uint64) (SegmentReader, error)
}

type fileSegmentReader struct {
	segId uint64
	file  *os.File
}

func NewFileSegmentReader(segId uint64, file *os.File) SegmentReader {
	return &fileSegmentReader{
		segId: segId,
		file:  file,
	}
}

func (sr *fileSegmentReader) SegID() uint64 {
	return sr.segId
}

func (sr *fileSegmentReader) SeekTo(offset int64) error {
	_, err := sr.file.Seek(offset, io.SeekStart)
	return err
}

func (sr *fileSegmentReader) Reader() io.Reader {
	return sr.file
}

func (sr *fileSegmentReader) Close() error {
	return sr.file.Close()
}
