package testutil

import (
	"errors"
	"io"

	"github.com/julianstephens/waldb/internal/waldb/wal"
)

// SegmentReader is a test implementation of wal.SegmentReader
type SegmentReader struct {
	SegmentID      uint64
	Data           []byte
	pos            int64
	Closed         bool
	SeekErr        error
	ReadErr        error
	CloseErr       error
	LastSeekOffset int64
	SeekCallCount  int
}

// SegID returns the segment ID
func (m *SegmentReader) SegID() uint64 {
	return m.SegmentID
}

// SeekTo seeks to the specified offset
func (m *SegmentReader) SeekTo(offset int64) error {
	if m.SeekErr != nil {
		return m.SeekErr
	}
	m.pos = offset
	m.LastSeekOffset = offset
	m.SeekCallCount++
	return nil
}

// Reader returns an io.Reader for the segment
func (m *SegmentReader) Reader() io.Reader {
	return &readerAdapter{data: m.Data[m.pos:], readErr: m.ReadErr}
}

// Close closes the segment reader
func (m *SegmentReader) Close() error {
	m.Closed = true
	return m.CloseErr
}

// readerAdapter adapts byte slice to io.Reader interface
type readerAdapter struct {
	data    []byte
	pos     int
	readErr error
}

func (r *readerAdapter) Read(b []byte) (int, error) {
	if r.readErr != nil {
		return 0, r.readErr
	}
	if r.pos >= len(r.data) {
		return 0, io.EOF
	}
	n := copy(b, r.data[r.pos:])
	r.pos += n
	return n, nil
}

// SegmentProvider is a test implementation of wal.SegmentProvider
type SegmentProvider struct {
	Segments map[uint64]wal.SegmentReader
	SegIDs   []uint64
	OpenErr  error
}

// NewSegmentProvider creates a new test segment provider
func NewSegmentProvider() *SegmentProvider {
	return &SegmentProvider{
		Segments: make(map[uint64]wal.SegmentReader),
		SegIDs:   []uint64{},
	}
}

// AddSegment adds a segment to the provider
func (p *SegmentProvider) AddSegment(segID uint64, data []byte) {
	p.Segments[segID] = &SegmentReader{
		SegmentID: segID,
		Data:      data,
	}
	p.SegIDs = append(p.SegIDs, segID)
}

// SetOpenError sets an error to be returned on OpenSegment calls
func (p *SegmentProvider) SetOpenError(err error) {
	p.OpenErr = err
}

// SegmentIDs returns the list of segment IDs
func (p *SegmentProvider) SegmentIDs() []uint64 {
	return p.SegIDs
}

// OpenSegment opens a segment and returns it or an error
func (p *SegmentProvider) OpenSegment(segID uint64) (wal.SegmentReader, error) {
	if p.OpenErr != nil {
		return nil, p.OpenErr
	}
	sr, ok := p.Segments[segID]
	if !ok {
		return nil, errors.New("segment not found")
	}
	return sr, nil
}
