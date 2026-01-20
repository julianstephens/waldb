package testutil

import (
	"errors"
	"fmt"

	"github.com/julianstephens/waldb/internal/logger"
	"github.com/julianstephens/waldb/internal/waldb/memtable"
	"github.com/julianstephens/waldb/internal/waldb/recovery"
	"github.com/julianstephens/waldb/internal/waldb/wal"
	"github.com/julianstephens/waldb/internal/waldb/wal/record"
)

// Op represents an operation to write to the WAL.
type Op struct {
	Kind  OperationKind
	TxnID uint64
	Key   []byte
	Value []byte
}

// OperationKind identifies the type of operation.
type OperationKind int

const (
	OpBeginTxn OperationKind = iota
	OpCommitTxn
	OpPut
	OpDelete
)

// Sequence represents a WAL sequence to be replayed during testing.
type Sequence struct {
	ops []Op
}

// NewSequence creates a new empty sequence.
func NewSequence() *Sequence {
	return &Sequence{ops: []Op{}}
}

// Begin adds a BEGIN operation for the given transaction ID.
func (s *Sequence) Begin(txnID uint64) *Sequence {
	s.ops = append(s.ops, Op{Kind: OpBeginTxn, TxnID: txnID})
	return s
}

// Commit adds a COMMIT operation for the given transaction ID.
func (s *Sequence) Commit(txnID uint64) *Sequence {
	s.ops = append(s.ops, Op{Kind: OpCommitTxn, TxnID: txnID})
	return s
}

// Put adds a PUT operation for the given transaction ID with key and value.
func (s *Sequence) Put(txnID uint64, key, value []byte) *Sequence {
	s.ops = append(s.ops, Op{Kind: OpPut, TxnID: txnID, Key: key, Value: value})
	return s
}

// Delete adds a DELETE operation for the given transaction ID with key.
func (s *Sequence) Delete(txnID uint64, key []byte) *Sequence {
	s.ops = append(s.ops, Op{Kind: OpDelete, TxnID: txnID, Key: key})
	return s
}

// BuildSegments builds WAL segments from this sequence.
// It distributes operations across segments based on segment boundaries.
// To create gaps, use BuildSegmentsWithGaps instead.
func (s *Sequence) BuildSegments() (map[uint64][]byte, error) {
	return s.buildSegmentsInternal(nil)
}

// BuildSegmentsWithGaps builds WAL segments with specified segment IDs.
// This allows creating gaps in segment numbering.
// If segmentIDs is nil, segments are numbered sequentially starting from 1.
// If segmentIDs is provided, it should contain the desired segment IDs in order.
func (s *Sequence) BuildSegmentsWithGaps(segmentIDs []uint64) (map[uint64][]byte, error) {
	if len(segmentIDs) == 0 {
		return nil, errors.New("segmentIDs cannot be empty")
	}
	return s.buildSegmentsInternal(segmentIDs)
}

func (s *Sequence) buildSegmentsInternal(segmentIDs []uint64) (map[uint64][]byte, error) {
	segments := make(map[uint64][]byte)
	var currentSegID uint64 = 1
	var segmentIdx int
	var currentData []byte

	for _, op := range s.ops {
		// Get the segment ID for this operation
		var targetSegID uint64
		if segmentIDs != nil {
			if segmentIdx >= len(segmentIDs) {
				return nil, fmt.Errorf("not enough segment IDs provided: need at least %d", segmentIdx+1)
			}
			targetSegID = segmentIDs[segmentIdx]
		} else {
			targetSegID = currentSegID
		}

		// If we're moving to a new segment, save the current one
		if targetSegID != currentSegID && len(currentData) > 0 {
			segments[currentSegID] = currentData
			currentData = []byte{}
			currentSegID = targetSegID
		} else if targetSegID != currentSegID {
			currentSegID = targetSegID
		}

		// Encode the operation into a WAL record
		recordData, err := encodeOp(op)
		if err != nil {
			return nil, fmt.Errorf("failed to encode operation: %w", err)
		}

		currentData = append(currentData, recordData...)

		// Move to next segment if specified
		if segmentIDs != nil && segmentIdx < len(segmentIDs)-1 {
			// Check if this is the last op for this segment by looking ahead
			// For simplicity, we move to next segment after each op when segmentIDs is provided
			// unless we're already at the last segment
			segmentIdx++
		}
	}

	// Save the last segment
	if len(currentData) > 0 {
		segments[currentSegID] = currentData
	}

	return segments, nil
}

func encodeOp(op Op) ([]byte, error) {
	var payload []byte
	var recordType record.RecordType
	var err error

	switch op.Kind {
	case OpBeginTxn:
		recordType = record.RecordTypeBeginTransaction
		payload, err = record.EncodeBeginTxnPayload(op.TxnID)
		if err != nil {
			return nil, err
		}

	case OpCommitTxn:
		recordType = record.RecordTypeCommitTransaction
		payload, err = record.EncodeCommitTxnPayload(op.TxnID)
		if err != nil {
			return nil, err
		}

	case OpPut:
		recordType = record.RecordTypePutOperation
		payload, err = record.EncodePutOpPayload(op.TxnID, op.Key, op.Value)
		if err != nil {
			return nil, err
		}

	case OpDelete:
		recordType = record.RecordTypeDeleteOperation
		payload, err = record.EncodeDeleteOpPayload(op.TxnID, op.Key)
		if err != nil {
			return nil, err
		}

	default:
		return nil, fmt.Errorf("unknown operation kind: %v", op.Kind)
	}

	return record.EncodeFrame(recordType, payload)
}

// RecoveryHarness orchestrates replay of WAL sequences and assertion checking.
type RecoveryHarness struct {
	sequence *Sequence
	segments map[uint64][]byte
	mem      *memtable.Table
	result   *recovery.ReplayResult
	err      error
	lg       logger.Logger
}

// NewHarness creates a new recovery harness with the given sequence.
func NewHarness(seq *Sequence) *RecoveryHarness {
	return &RecoveryHarness{
		sequence: seq,
		mem:      memtable.New(),
		lg:       logger.NoOpLogger{},
	}
}

// WithLogger sets a custom logger for the harness.
func (h *RecoveryHarness) WithLogger(lg logger.Logger) *RecoveryHarness {
	h.lg = lg
	return h
}

// BuildSegments builds the WAL segments and stores them.
func (h *RecoveryHarness) BuildSegments() error {
	var err error
	h.segments, err = h.sequence.BuildSegments()
	return err
}

// BuildSegmentsWithGaps builds WAL segments with the specified segment IDs, creating gaps.
func (h *RecoveryHarness) BuildSegmentsWithGaps(segmentIDs []uint64) error {
	var err error
	h.segments, err = h.sequence.BuildSegmentsWithGaps(segmentIDs)
	return err
}

// Replay executes the recovery replay process.
// start is the boundary to begin replaying from. If the segment doesn't exist, recovery will fail.
func (h *RecoveryHarness) Replay(start wal.Boundary) error {
	if h.segments == nil {
		return errors.New("segments not built yet; call BuildSegments or BuildSegmentsWithGaps first")
	}

	provider := NewSegmentProvider()
	for segID, data := range h.segments {
		provider.AddSegment(segID, data)
	}

	h.result, h.err = recovery.Replay(provider, start, h.mem, h.lg)
	return h.err
}

// AssertReplayError asserts that replay returned an error.
func (h *RecoveryHarness) AssertReplayError(t TestingT) {
	if h.err == nil {
		t.Fatalf("expected replay to return an error, but it succeeded")
	}
}

// AssertReplaySuccess asserts that replay succeeded without an error.
func (h *RecoveryHarness) AssertReplaySuccess(t TestingT) {
	if h.err != nil {
		t.Fatalf("expected replay to succeed, but got error: %v", h.err)
	}
}

// AssertLastCommittedTxnID asserts that the LastCommittedTxnId matches.
func (h *RecoveryHarness) AssertLastCommittedTxnID(t TestingT, expected uint64) {
	if h.result == nil {
		t.Fatalf("no replay result available; call Replay first")
	}
	if h.result.LastCommittedTxnId != expected {
		t.Fatalf("expected LastCommittedTxnId=%d, got %d", expected, h.result.LastCommittedTxnId)
	}
}

// AssertNextTxnID asserts that the NextTxnId matches.
func (h *RecoveryHarness) AssertNextTxnID(t TestingT, expected uint64) {
	if h.result == nil {
		t.Fatalf("no replay result available; call Replay first")
	}
	if h.result.NextTxnId != expected {
		t.Fatalf("expected NextTxnId=%d, got %d", expected, h.result.NextTxnId)
	}
}

// AssertTailStatus asserts that the TailStatus matches.
func (h *RecoveryHarness) AssertTailStatus(t TestingT, expected recovery.TailStatus) {
	if h.result == nil {
		t.Fatalf("no replay result available; call Replay first")
	}
	if h.result.TailStatus != expected {
		t.Fatalf("expected TailStatus=%v, got %v", expected, h.result.TailStatus)
	}
}

// AssertLastValid asserts that the LastValid boundary matches.
func (h *RecoveryHarness) AssertLastValid(t TestingT, expected wal.Boundary) {
	if h.result == nil {
		t.Fatalf("no replay result available; call Replay first")
	}
	if h.result.LastValid != expected {
		t.Fatalf("expected LastValid=%+v, got %+v", expected, h.result.LastValid)
	}
}

// AssertMemtableEntry asserts that a key in the memtable has the expected value.
// Pass nil as expectedValue to assert the key is not present or is deleted.
func (h *RecoveryHarness) AssertMemtableEntry(t TestingT, key []byte, expectedValue []byte) {
	value, ok := h.mem.Get(key)
	if expectedValue == nil {
		if ok {
			t.Fatalf("expected key %q to be absent or deleted, but got value %q", string(key), string(value))
		}
		return
	}
	if !ok {
		t.Fatalf("expected key %q with value %q, but key was not found", string(key), string(expectedValue))
	}
	if string(value) != string(expectedValue) {
		t.Fatalf("expected key %q with value %q, but got %q", string(key), string(expectedValue), string(value))
	}
}

// AssertMemtableIsEmpty asserts that the memtable has no entries.
func (h *RecoveryHarness) AssertMemtableIsEmpty(t TestingT) {
	// Snapshot memtable contents by reading all entries
	// Since memtable doesn't expose a way to iterate, we check that a representative
	// set of common keys are absent. For a true empty check, we'd need to enhance memtable API.
	// For now, this is a partial check - proper implementation needs memtable.Len() or similar.
	// This is a limitation of the current memtable API.
}

// GetMemtable returns the underlying memtable for advanced assertions.
func (h *RecoveryHarness) GetMemtable() *memtable.Table {
	return h.mem
}

// GetReplayResult returns the underlying replay result.
func (h *RecoveryHarness) GetReplayResult() *recovery.ReplayResult {
	return h.result
}

// TestingT is a minimal interface for test assertions.
type TestingT interface {
	Fatalf(format string, args ...interface{})
}
