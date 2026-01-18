package wal

import (
	"io"
	"os"
)

type FileSegmentReader struct {
	segId uint64
	file  *os.File
}

func NewFileSegmentReader(segId uint64, file *os.File) *FileSegmentReader {
	return &FileSegmentReader{
		segId: segId,
		file:  file,
	}
}

func (sr *FileSegmentReader) SegID() uint64 {
	return sr.segId
}

func (sr *FileSegmentReader) SeekTo(offset int64) error {
	_, err := sr.file.Seek(offset, io.SeekStart)
	return err
}

func (sr *FileSegmentReader) Reader() io.Reader {
	return sr.file
}

func (sr *FileSegmentReader) Close() error {
	return sr.file.Close()
}
