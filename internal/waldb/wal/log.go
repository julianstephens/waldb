package wal

import (
	"fmt"
	"os"
	"path"
	"slices"
	"sync"

	"github.com/julianstephens/go-utils/helpers"
	"github.com/julianstephens/waldb/internal/waldb/wal/record"
)

type LogOpts struct {
	// 0 means "never rotate" (single segment)
	SegmentMaxBytes int64
}

type Log struct {
	mu sync.Mutex

	dir  string
	opts LogOpts

	// segments is always kept sorted for binary search
	segments    []uint64 // sorted ascending; includes activeSegId
	activeSegId uint64
	active      *SegmentAppender

	closed bool
}

func logClosed(dir string) error {
	return &LogError{
		Err: ErrWALClosed,
		Dir: dir,
		Op:  "log",
	}
}

func wrapLogErr(op string, sentinel error, dir string, segID uint64, cause error) error {
	return &LogError{
		Err:   sentinel,
		Dir:   dir,
		SegID: segID,
		Op:    op,
		Cause: cause,
	}
}

func listSegments(dir string) ([]uint64, error) {
	files, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var segs []uint64
	for _, fi := range files {
		if fi.IsDir() {
			continue
		}
		var segId uint64
		n, err := fmt.Sscanf(fi.Name(), "segment-%020d.wal", &segId)
		if err != nil || n != 1 {
			continue
		}
		segs = append(segs, segId)
	}
	return segs, nil
}

// OpenLog opens or creates the WAL directory, discovers existing segments,
// selects the active segment, and prepares it for append.
func OpenLog(dir string, opts LogOpts) (*Log, error) {
	mgr := &Log{
		mu:     sync.Mutex{},
		dir:    dir,
		opts:   opts,
		closed: false,
	}

	if err := helpers.Ensure(dir, true); err != nil {
		return nil, wrapLogErr("ensure_wal_dir", ErrInvalidWALDir, dir, 0, err)
	}

	segs, err := listSegments(dir)
	if err != nil {
		return nil, wrapLogErr("list_segments", ErrSegmentList, dir, 0, err)
	}

	mgr.segments = segs
	mgr.sortSegments()

	var activeSegId uint64
	var activeFile *os.File

	if len(mgr.segments) == 0 {
		// No segments exist; create the first one
		activeFile, activeSegId, err = mgr.createNextSegment(dir, 0)
		if err != nil {
			return nil, wrapLogErr("create_segment", ErrSegmentCreate, dir, 0, err)
		}
		mgr.segments = append(mgr.segments, activeSegId)
		mgr.sortSegments()
	} else {
		activeSegId = mgr.segments[len(mgr.segments)-1]
		activeSegPath := mgr.segmentPath(dir, activeSegId, true)
		if activeSegPath == "" {
			return nil, wrapLogErr("get_segment_path", ErrSegmentNotFound, dir, activeSegId, nil)
		}
		activeFile, err = mgr.openSegmentForAppend(activeSegPath)
		if err != nil {
			return nil, wrapLogErr("open_segment", ErrSegmentOpen, dir, activeSegId, err)
		}
	}

	if activeFile == nil {
		return nil, wrapLogErr("open_segment", ErrNilSegmentFile, dir, activeSegId, nil)
	}

	activeWriter, err := NewSegmentAppender(activeFile)
	if err != nil {
		return nil, wrapLogErr("create_segment_writer", ErrSegmentOpen, dir, activeSegId, err)
	}

	mgr.activeSegId = activeSegId
	mgr.active = activeWriter.(*SegmentAppender)

	return mgr, nil
}

func (m *Log) SegmentIDs() []uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return slices.Clone(m.segments)
}

func (m *Log) SegmentPath(segId uint64) string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.segmentPath(m.dir, segId, true)
}

func (m *Log) OpenSegment(segId uint64) (SegmentReader, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	segPath := m.segmentPath(m.dir, segId, true)
	if segPath == "" {
		return nil, wrapLogErr("get_segment_path", ErrSegmentNotFound, m.dir, segId, nil)
	}

	file, err := os.Open(segPath) //nolint:gosec
	if err != nil {
		return nil, wrapLogErr("open_segment", ErrSegmentOpen, m.dir, segId, err)
	}

	return NewFileSegmentReader(segId, file), nil
}

// Append appends a single framed WAL record to the active segment,
// rotating segments if policy requires.
func (m *Log) Append(rt record.RecordType, payload []byte) (offset int64, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed {
		err = logClosed(m.dir)
		return
	}

	if err2 := m.maybeRotateLocked(len(payload)); err2 != nil {
		err = wrapLogErr("rotate_segment", ErrSegmentRotate, m.dir, m.activeSegId, err2)
		return
	}

	offset, err2 := m.active.Append(rt, payload)
	if err2 != nil {
		err = wrapLogErr("append_record", ErrAppendFailed, m.dir, m.activeSegId, err2)
		return
	}

	return
}

// Flush flushes buffered writes of the active segment.
func (m *Log) Flush() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed {
		return logClosed(m.dir)
	}

	if err := m.active.Flush(); err != nil {
		return wrapLogErr("flush_segment", ErrSegmentFlush, m.dir, m.activeSegId, err)
	}

	return nil
}

// FSync flushes then fsyncs the active segment.
func (m *Log) FSync() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed {
		return logClosed(m.dir)
	}

	if err := m.active.FSync(); err != nil {
		return wrapLogErr("fsync_segment", ErrSegmentSync, m.dir, m.activeSegId, err)
	}

	return nil
}

// Close closes the active segment writer and marks the log closed.
func (m *Log) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed {
		return nil
	}
	m.closed = true

	if err := m.active.Close(); err != nil {
		return wrapLogErr("close_segment", ErrSegmentClose, m.dir, m.activeSegId, err)
	}

	return nil
}

// sortSegments sorts the segment IDs in ascending order.
func (m *Log) sortSegments() {
	slices.SortFunc(m.segments, func(a, b uint64) int {
		if a < b {
			return -1
		} else if a > b {
			return 1
		}
		return 0
	})
}

// segmentPath returns the path for the given segment ID.
// If shouldExist is true, it returns an empty string if the segment ID is not found.
func (m *Log) segmentPath(dir string, segId uint64, shouldExist bool) string {
	if _, exists := slices.BinarySearch(m.segments, segId); shouldExist && !exists {
		return ""
	}
	return path.Join(dir, fmt.Sprintf("segment-%020d.wal", segId))
}

// openSegmentForAppend opens the segment file at the given path for appending.
func (m *Log) openSegmentForAppend(path string) (*os.File, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o600) //nolint:gosec
	if err != nil {
		return nil, err
	}
	return file, nil
}

// createNextSegment creates a new segment file with the next segment ID.
func (m *Log) createNextSegment(dir string, segId uint64) (*os.File, uint64, error) {
	path := m.segmentPath(dir, segId+1, false)
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_EXCL, 0o600) //nolint:gosec
	if err != nil {
		return nil, 0, err
	}
	return file, segId + 1, nil
}

// maybeRotateLocked decides whether to rotate before appending the next record
func (m *Log) maybeRotateLocked(payloadLen int) error {
	// No rotation if SegmentMaxBytes == 0
	if m.opts.SegmentMaxBytes <= 0 {
		return nil
	}
	// Check if active segment has enough space
	// payload validated in Append
	if m.active.CurrentOffset()+record.EncodedRecordSize(payloadLen) <= m.opts.SegmentMaxBytes {
		return nil
	}

	// Rotate segment (flushes but does not fsync)
	if err := m.active.Close(); err != nil {
		return err
	}
	newFile, newSegId, err := m.createNextSegment(m.dir, m.activeSegId)
	if err != nil {
		return err
	}
	newWriter, err := NewSegmentAppender(newFile)
	if err != nil {
		return err
	}

	m.segments = append(m.segments, newSegId)
	m.sortSegments()
	m.activeSegId = newSegId
	m.active = newWriter.(*SegmentAppender)

	return nil
}
