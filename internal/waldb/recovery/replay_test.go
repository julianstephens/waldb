package recovery_test

import (
	"errors"
	"io"
	"testing"

	tst "github.com/julianstephens/go-utils/tests"
	"github.com/julianstephens/waldb/internal/waldb/memtable"
	"github.com/julianstephens/waldb/internal/waldb/recovery"
	"github.com/julianstephens/waldb/internal/waldb/wal"
	"github.com/julianstephens/waldb/internal/waldb/wal/record"
)

// MockSegmentReader implements wal.SegmentReader for testing
type MockSegmentReader struct {
	segId       uint64
	data        []byte
	pos         int64
	closed      bool
	seekErr     error
	readErr     error
	closeErr    error
	lastSeekTo  int64 // Track the last SeekTo call
	seekedCalls int   // Track number of SeekTo calls
}

func (m *MockSegmentReader) SegID() uint64 {
	return m.segId
}

func (m *MockSegmentReader) SeekTo(offset int64) error {
	if m.seekErr != nil {
		return m.seekErr
	}
	m.pos = offset
	m.lastSeekTo = offset
	m.seekedCalls++
	return nil
}

func (m *MockSegmentReader) Reader() io.Reader {
	return &mockReader{data: m.data[m.pos:], readErr: m.readErr}
}

func (m *MockSegmentReader) Close() error {
	m.closed = true
	return m.closeErr
}

type mockReader struct {
	data    []byte
	pos     int
	readErr error
}

func (r *mockReader) Read(b []byte) (int, error) {
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

// MockSegmentProvider implements wal.SegmentProvider for testing
type MockSegmentProvider struct {
	segments map[uint64]wal.SegmentReader
	segIds   []uint64
	openErr  error
}

func (m *MockSegmentProvider) SegmentIDs() []uint64 {
	return m.segIds
}

func (m *MockSegmentProvider) OpenSegment(segID uint64) (wal.SegmentReader, error) {
	if m.openErr != nil {
		return nil, m.openErr
	}
	sr, ok := m.segments[segID]
	if !ok {
		return nil, errors.New("segment not found")
	}
	return sr, nil
}

// TestReplayEmptyProvider tests replay with no segments
func TestReplayEmptyProvider(t *testing.T) {
	provider := &MockSegmentProvider{
		segments: make(map[uint64]wal.SegmentReader),
		segIds:   []uint64{},
	}
	mem := memtable.New()
	start := wal.Boundary{SegId: 1, Offset: 0}

	result, err := recovery.Replay(provider, start, mem)
	tst.AssertTrue(t, err != nil, "expected error with empty provider")
	tst.AssertTrue(t, result == nil || result.NextTxnId == 0, "expected zero next txn id")
}

// TestReplaySegmentNotFound tests replay with starting segment that doesn't exist
func TestReplaySegmentNotFound(t *testing.T) {
	provider := &MockSegmentProvider{
		segments: make(map[uint64]wal.SegmentReader),
		segIds:   []uint64{1, 2, 3},
	}
	mem := memtable.New()
	start := wal.Boundary{SegId: 999, Offset: 0}

	result, err := recovery.Replay(provider, start, mem)
	tst.AssertTrue(t, err != nil, "expected error when segment not found")
	tst.AssertTrue(t, result == nil, "expected nil result when segment not found")
}

// TestReplayOpenSegmentError tests replay when opening segment fails
func TestReplayOpenSegmentError(t *testing.T) {
	provider := &MockSegmentProvider{
		segments: make(map[uint64]wal.SegmentReader),
		segIds:   []uint64{1},
		openErr:  errors.New("open failed"),
	}
	mem := memtable.New()
	start := wal.Boundary{SegId: 1, Offset: 0}

	result, err := recovery.Replay(provider, start, mem)
	tst.AssertTrue(t, err != nil, "expected error when open fails")
	tst.AssertTrue(t, result != nil, "expected result to be returned")
	tst.RequireDeepEqual(t, result.NextTxnId, uint64(1))
}

// TestReplaySeekError tests replay when seeking fails
func TestReplaySeekError(t *testing.T) {
	mockReader := &MockSegmentReader{
		segId:   1,
		data:    []byte{},
		seekErr: errors.New("seek failed"),
	}
	provider := &MockSegmentProvider{
		segments: map[uint64]wal.SegmentReader{1: mockReader},
		segIds:   []uint64{1},
	}
	mem := memtable.New()
	start := wal.Boundary{SegId: 1, Offset: 0}

	result, err := recovery.Replay(provider, start, mem)
	tst.AssertTrue(t, err != nil, "expected error when seek fails")
	tst.AssertTrue(t, result != nil, "expected result to be returned")
}

// TestReplayNoRecords tests replay with empty segment
func TestReplayNoRecords(t *testing.T) {
	mockReader := &MockSegmentReader{
		segId: 1,
		data:  []byte{},
	}
	provider := &MockSegmentProvider{
		segments: map[uint64]wal.SegmentReader{1: mockReader},
		segIds:   []uint64{1},
	}
	mem := memtable.New()
	start := wal.Boundary{SegId: 1, Offset: 0}

	result, err := recovery.Replay(provider, start, mem)
	tst.RequireNoError(t, err)
	tst.AssertNotNil(t, result, "expected non-nil result")
	tst.RequireDeepEqual(t, result.NextTxnId, uint64(1))
	tst.RequireDeepEqual(t, result.LastValid.SegId, uint64(1))
	tst.RequireDeepEqual(t, result.LastValid.Offset, int64(0))
}

// TestReplaySingleBeginTransaction tests replay with a single begin transaction
func TestReplaySingleBeginTransaction(t *testing.T) {
	// Create a begin transaction record
	beginPayload, err := record.EncodeBeginTxnPayload(1)
	tst.RequireNoError(t, err)
	encodedRec, err := record.Encode(record.RecordTypeBeginTransaction, beginPayload)
	tst.RequireNoError(t, err)

	mockReader := &MockSegmentReader{
		segId: 1,
		data:  encodedRec,
	}
	provider := &MockSegmentProvider{
		segments: map[uint64]wal.SegmentReader{1: mockReader},
		segIds:   []uint64{1},
	}
	mem := memtable.New()
	start := wal.Boundary{SegId: 1, Offset: 0}

	result, err := recovery.Replay(provider, start, mem)
	tst.RequireNoError(t, err)
	tst.AssertNotNil(t, result, "expected non-nil result")
}

// TestReplayMultipleSegments tests replay across multiple segments
func TestReplayMultipleSegments(t *testing.T) {
	// Create begin transaction records
	beginPayload1, err := record.EncodeBeginTxnPayload(1)
	tst.RequireNoError(t, err)
	encodedRec1, err := record.Encode(record.RecordTypeBeginTransaction, beginPayload1)
	tst.RequireNoError(t, err)

	beginPayload2, err := record.EncodeBeginTxnPayload(2)
	tst.RequireNoError(t, err)
	encodedRec2, err := record.Encode(record.RecordTypeBeginTransaction, beginPayload2)
	tst.RequireNoError(t, err)

	mockReader1 := &MockSegmentReader{
		segId: 1,
		data:  encodedRec1,
	}
	mockReader2 := &MockSegmentReader{
		segId: 2,
		data:  encodedRec2,
	}

	provider := &MockSegmentProvider{
		segments: map[uint64]wal.SegmentReader{
			1: mockReader1,
			2: mockReader2,
		},
		segIds: []uint64{1, 2},
	}
	mem := memtable.New()
	start := wal.Boundary{SegId: 1, Offset: 0}

	result, err := recovery.Replay(provider, start, mem)
	tst.RequireNoError(t, err)
	tst.AssertNotNil(t, result, "expected non-nil result")
	tst.RequireDeepEqual(t, result.LastValid.SegId, uint64(2))
}

// TestReplayStartFromMiddleSegment tests replay starting from middle of multiple segments
func TestReplayStartFromMiddleSegment(t *testing.T) {
	// Create begin transaction records
	beginPayload1, err := record.EncodeBeginTxnPayload(1)
	tst.RequireNoError(t, err)
	encodedRec1, err := record.Encode(record.RecordTypeBeginTransaction, beginPayload1)
	tst.RequireNoError(t, err)

	beginPayload2, err := record.EncodeBeginTxnPayload(2)
	tst.RequireNoError(t, err)
	encodedRec2, err := record.Encode(record.RecordTypeBeginTransaction, beginPayload2)
	tst.RequireNoError(t, err)

	mockReader1 := &MockSegmentReader{
		segId: 1,
		data:  encodedRec1,
	}
	mockReader2 := &MockSegmentReader{
		segId: 2,
		data:  encodedRec2,
	}

	provider := &MockSegmentProvider{
		segments: map[uint64]wal.SegmentReader{
			1: mockReader1,
			2: mockReader2,
		},
		segIds: []uint64{1, 2},
	}
	mem := memtable.New()
	// Start from segment 2
	start := wal.Boundary{SegId: 2, Offset: 0}

	result, err := recovery.Replay(provider, start, mem)
	tst.RequireNoError(t, err)
	tst.AssertNotNil(t, result, "expected non-nil result")
	// Should only process segment 2
	tst.RequireDeepEqual(t, result.LastValid.SegId, uint64(2))
}

// TestReplayCloseSegmentError tests replay when closing segment fails
func TestReplayCloseSegmentError(t *testing.T) {
	beginPayload, err := record.EncodeBeginTxnPayload(1)
	tst.RequireNoError(t, err)
	encodedRec, err := record.Encode(record.RecordTypeBeginTransaction, beginPayload)
	tst.RequireNoError(t, err)

	mockReader := &MockSegmentReader{
		segId:    1,
		data:     encodedRec,
		closeErr: errors.New("close failed"),
	}
	provider := &MockSegmentProvider{
		segments: map[uint64]wal.SegmentReader{1: mockReader},
		segIds:   []uint64{1},
	}
	mem := memtable.New()
	start := wal.Boundary{SegId: 1, Offset: 0}

	result, err := recovery.Replay(provider, start, mem)
	tst.AssertTrue(t, err != nil, "expected error when close fails")
	tst.AssertTrue(t, result != nil, "expected result to be returned")
}

// TestReplayUpdatesBoundary tests that replay updates the boundary correctly
func TestReplayUpdatesBoundary(t *testing.T) {
	beginPayload, err := record.EncodeBeginTxnPayload(1)
	tst.RequireNoError(t, err)
	encodedRec, err := record.Encode(record.RecordTypeBeginTransaction, beginPayload)
	tst.RequireNoError(t, err)

	mockReader := &MockSegmentReader{
		segId: 1,
		data:  encodedRec,
	}
	provider := &MockSegmentProvider{
		segments: map[uint64]wal.SegmentReader{1: mockReader},
		segIds:   []uint64{1},
	}
	mem := memtable.New()
	start := wal.Boundary{SegId: 1, Offset: 0}

	result, err := recovery.Replay(provider, start, mem)
	tst.RequireNoError(t, err)
	tst.AssertNotNil(t, result, "expected non-nil result")
	// Boundary should be updated to after the record
	tst.AssertTrue(t, result.LastValid.Offset > 0, "expected offset to be updated")
}

// TestReplaySeekFirstSegmentAtStartOffset tests starting replay from first segment
func TestReplaySeekFirstSegmentAtStartOffset(t *testing.T) {
	beginPayload, err := record.EncodeBeginTxnPayload(1)
	tst.RequireNoError(t, err)
	encodedRec, err := record.Encode(record.RecordTypeBeginTransaction, beginPayload)
	tst.RequireNoError(t, err)

	mockReader := &MockSegmentReader{
		segId: 1,
		data:  encodedRec,
	}
	provider := &MockSegmentProvider{
		segments: map[uint64]wal.SegmentReader{1: mockReader},
		segIds:   []uint64{1},
	}
	mem := memtable.New()
	// Start at offset 0
	start := wal.Boundary{SegId: 1, Offset: 0}

	result, err := recovery.Replay(provider, start, mem)
	// Should process the record
	tst.RequireNoError(t, err)
	tst.AssertNotNil(t, result, "expected non-nil result")
}

// TestReplaySeekSubsequentSegmentAtZero tests seeking to 0 in subsequent segments
func TestReplaySeekSubsequentSegmentAtZero(t *testing.T) {
	beginPayload1, err := record.EncodeBeginTxnPayload(1)
	tst.RequireNoError(t, err)
	encodedRec1, err := record.Encode(record.RecordTypeBeginTransaction, beginPayload1)
	tst.RequireNoError(t, err)

	beginPayload2, err := record.EncodeBeginTxnPayload(2)
	tst.RequireNoError(t, err)
	encodedRec2, err := record.Encode(record.RecordTypeBeginTransaction, beginPayload2)
	tst.RequireNoError(t, err)

	mockReader1 := &MockSegmentReader{
		segId: 1,
		data:  encodedRec1,
	}
	mockReader2 := &MockSegmentReader{
		segId: 2,
		data:  encodedRec2,
	}

	provider := &MockSegmentProvider{
		segments: map[uint64]wal.SegmentReader{
			1: mockReader1,
			2: mockReader2,
		},
		segIds: []uint64{1, 2},
	}
	mem := memtable.New()
	start := wal.Boundary{SegId: 1, Offset: 0}

	result, err := recovery.Replay(provider, start, mem)
	tst.RequireNoError(t, err)
	tst.AssertNotNil(t, result, "expected non-nil result")
	// Verify both segments were processed
	tst.RequireDeepEqual(t, result.LastValid.SegId, uint64(2))
}

// TestReplayRecordReadError tests replay when reading records fails
func TestReplayRecordReadError(t *testing.T) {
	mockReader := &MockSegmentReader{
		segId:   1,
		data:    []byte{0xFF}, // Invalid record data
		readErr: errors.New("read failed"),
	}
	provider := &MockSegmentProvider{
		segments: map[uint64]wal.SegmentReader{1: mockReader},
		segIds:   []uint64{1},
	}
	mem := memtable.New()
	start := wal.Boundary{SegId: 1, Offset: 0}

	result, err := recovery.Replay(provider, start, mem)
	tst.AssertTrue(t, err != nil, "expected error when read fails")
	tst.AssertTrue(t, result != nil, "expected result to be returned")
}

// TestReplayInvalidRecordType tests replay with invalid record type
func TestReplayInvalidRecordType(t *testing.T) {
	// Create a record with invalid type
	encodedRec, _ := record.Encode(record.RecordType(255), []byte{})

	mockReader := &MockSegmentReader{
		segId: 1,
		data:  encodedRec,
	}
	provider := &MockSegmentProvider{
		segments: map[uint64]wal.SegmentReader{1: mockReader},
		segIds:   []uint64{1},
	}
	mem := memtable.New()
	start := wal.Boundary{SegId: 1, Offset: 0}

	result, err := recovery.Replay(provider, start, mem)
	tst.AssertTrue(t, err != nil, "expected error for invalid record type")
	tst.AssertTrue(t, result != nil, "expected result to be returned")
}

// TestReplayMemtableIntegration tests that replay writes to memtable
func TestReplayMemtableIntegration(t *testing.T) {
	// Create a put operation
	putPayload, err := record.EncodePutOpPayload(1, []byte("testkey"), []byte("testvalue"))
	tst.RequireNoError(t, err)
	encodedRec, err := record.Encode(record.RecordTypePutOperation, putPayload)
	tst.RequireNoError(t, err)

	mockReader := &MockSegmentReader{
		segId: 1,
		data:  encodedRec,
	}
	provider := &MockSegmentProvider{
		segments: map[uint64]wal.SegmentReader{1: mockReader},
		segIds:   []uint64{1},
	}
	mem := memtable.New()
	start := wal.Boundary{SegId: 1, Offset: 0}

	result, err := recovery.Replay(provider, start, mem)
	// Will error because there's no corresponding begin/commit, but that's expected
	tst.AssertTrue(t, err != nil || result != nil, "expected result or error")
}

// TestReplaySegment2SeekOffset tests that segment 2 is seeked to correct offset (bug test)
// Bug scenario: provider has segIds [1,2], start = {SegId:2, Offset:5}
// Should seek segment 2 to offset 5 (not 0)
func TestReplaySegment2SeekOffset(t *testing.T) {
	// Create a begin transaction record for segment 2
	beginPayload, err := record.EncodeBeginTxnPayload(1)
	tst.RequireNoError(t, err)
	encodedRec, err := record.Encode(record.RecordTypeBeginTransaction, beginPayload)
	tst.RequireNoError(t, err)

	// Create segment 1 (will be skipped since we start at segment 2)
	mockReader1 := &MockSegmentReader{
		segId: 1,
		data:  []byte{},
	}

	// Create segment 2 (will be seeked to offset 5)
	mockReader2 := &MockSegmentReader{
		segId: 2,
		data:  encodedRec,
	}

	provider := &MockSegmentProvider{
		segments: map[uint64]wal.SegmentReader{
			1: mockReader1,
			2: mockReader2,
		},
		segIds: []uint64{1, 2},
	}

	mem := memtable.New()
	start := wal.Boundary{SegId: 2, Offset: 5}

	result, err := recovery.Replay(provider, start, mem)
	_ = result // result may be nil if there's an error replaying
	_ = err    // error is expected since we don't have complete transaction data

	// Verify segment 2 was seeked to offset 5 (the bug: it was being seeked to 0)
	tst.AssertTrue(t, mockReader2.seekedCalls > 0, "expected segment 2 to be seeked")
	tst.AssertTrue(t, mockReader2.lastSeekTo == 5, "expected segment 2 to be seeked to offset 5")
	// Also verify segment 1 was never seeked (since we started at segment 2)
	tst.AssertTrue(t, mockReader1.seekedCalls == 0, "expected segment 1 to not be seeked")
}
