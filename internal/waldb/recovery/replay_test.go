package recovery_test

import (
	"errors"
	"testing"

	tst "github.com/julianstephens/go-utils/tests"
	"github.com/julianstephens/waldb/internal/testutil"
	"github.com/julianstephens/waldb/internal/waldb/memtable"
	"github.com/julianstephens/waldb/internal/waldb/recovery"
	"github.com/julianstephens/waldb/internal/waldb/wal"
	"github.com/julianstephens/waldb/internal/waldb/wal/record"
)

// TestReplayEmptyProvider tests replay with no segments
func TestReplayEmptyProvider(t *testing.T) {
	provider := testutil.NewSegmentProvider()
	mem := memtable.New()
	start := wal.Boundary{SegId: 1, Offset: 0}

	result, err := recovery.Replay(provider, start, mem)
	// Empty provider means no segments, so we expect an error
	tst.AssertTrue(t, err != nil, "expected error with empty provider")
	tst.AssertTrue(t, result == nil, "expected nil result")
}

// TestReplaySegmentNotFound tests replay with starting segment that doesn't exist
func TestReplaySegmentNotFound(t *testing.T) {
	provider := testutil.NewSegmentProvider()
	provider.AddSegment(1, []byte{})
	provider.AddSegment(2, []byte{})

	mem := memtable.New()
	// Start at segment 999 which doesn't exist
	start := wal.Boundary{SegId: 999, Offset: 0}

	result, err := recovery.Replay(provider, start, mem)
	tst.AssertTrue(t, err != nil, "expected error when segment not found")
	tst.AssertTrue(t, result == nil, "expected nil result")
}

// TestReplayOpenSegmentError tests replay when opening segment fails
func TestReplayOpenSegmentError(t *testing.T) {
	provider := testutil.NewSegmentProvider()
	provider.AddSegment(1, []byte{})
	provider.SetOpenError(errors.New("open failed"))

	mem := memtable.New()
	start := wal.Boundary{SegId: 1, Offset: 0}

	_, err := recovery.Replay(provider, start, mem)
	tst.AssertTrue(t, err != nil, "expected error when opening segment fails")
}

// TestReplaySingleBeginCommit tests replay with one begin/commit transaction
func TestReplaySingleBeginCommit(t *testing.T) {
	// Encode begin transaction record
	beginPayload, err := record.EncodeBeginTxnPayload(1)
	tst.RequireNoError(t, err)
	beginFrame, err := record.EncodeFrame(record.RecordTypeBeginTransaction, beginPayload)
	tst.RequireNoError(t, err)

	// Encode commit transaction record
	commitPayload, err := record.EncodeCommitTxnPayload(1)
	tst.RequireNoError(t, err)
	commitFrame, err := record.EncodeFrame(record.RecordTypeCommitTransaction, commitPayload)
	tst.RequireNoError(t, err)

	provider := testutil.NewSegmentProvider()
	provider.AddSegment(1, append(beginFrame, commitFrame...))

	mem := memtable.New()
	start := wal.Boundary{SegId: 1, Offset: 0}
	result, err := recovery.Replay(provider, start, mem)

	tst.RequireNoError(t, err)
	tst.AssertTrue(t, result != nil, "expected non-nil result")
	tst.AssertTrue(t, result.NextTxnId == 2, "expected NextTxnId to be 2")
}

// TestReplaySinglePutOperation tests replay with one put operation
func TestReplaySinglePutOperation(t *testing.T) {
	// Encode begin transaction
	beginPayload, err := record.EncodeBeginTxnPayload(1)
	tst.RequireNoError(t, err)
	beginFrame, err := record.EncodeFrame(record.RecordTypeBeginTransaction, beginPayload)
	tst.RequireNoError(t, err)

	// Encode put operation
	putPayload, err := record.EncodePutOpPayload(1, []byte("key1"), []byte("value1"))
	tst.RequireNoError(t, err)
	putFrame, err := record.EncodeFrame(record.RecordTypePutOperation, putPayload)
	tst.RequireNoError(t, err)

	// Encode commit transaction
	commitPayload, err := record.EncodeCommitTxnPayload(1)
	tst.RequireNoError(t, err)
	commitFrame, err := record.EncodeFrame(record.RecordTypeCommitTransaction, commitPayload)
	tst.RequireNoError(t, err)

	provider := testutil.NewSegmentProvider()
	provider.AddSegment(1, append(append(beginFrame, putFrame...), commitFrame...))

	mem := memtable.New()
	start := wal.Boundary{SegId: 1, Offset: 0}
	result, err := recovery.Replay(provider, start, mem)

	tst.RequireNoError(t, err)
	tst.AssertTrue(t, result != nil, "expected non-nil result")

	// Verify the key was written to memtable
	val, found := mem.Get([]byte("key1"))
	tst.AssertTrue(t, found, "expected key1 to be in memtable")
	tst.RequireDeepEqual(t, val, []byte("value1"))
}

// TestReplayMultiplePutOperations tests replay with multiple put operations in one transaction
func TestReplayMultiplePutOperations(t *testing.T) {
	// Encode begin transaction
	beginPayload, err := record.EncodeBeginTxnPayload(1)
	tst.RequireNoError(t, err)
	beginFrame, err := record.EncodeFrame(record.RecordTypeBeginTransaction, beginPayload)
	tst.RequireNoError(t, err)

	// Encode first put operation
	putPayload1, err := record.EncodePutOpPayload(1, []byte("key1"), []byte("value1"))
	tst.RequireNoError(t, err)
	putFrame1, err := record.EncodeFrame(record.RecordTypePutOperation, putPayload1)
	tst.RequireNoError(t, err)

	// Encode second put operation
	putPayload2, err := record.EncodePutOpPayload(1, []byte("key2"), []byte("value2"))
	tst.RequireNoError(t, err)
	putFrame2, err := record.EncodeFrame(record.RecordTypePutOperation, putPayload2)
	tst.RequireNoError(t, err)

	// Encode commit transaction
	commitPayload, err := record.EncodeCommitTxnPayload(1)
	tst.RequireNoError(t, err)
	commitFrame, err := record.EncodeFrame(record.RecordTypeCommitTransaction, commitPayload)
	tst.RequireNoError(t, err)

	provider := testutil.NewSegmentProvider()
	provider.AddSegment(1, append(append(append(beginFrame, putFrame1...), putFrame2...), commitFrame...))

	mem := memtable.New()
	start := wal.Boundary{SegId: 1, Offset: 0}
	result, err := recovery.Replay(provider, start, mem)

	tst.RequireNoError(t, err)
	tst.AssertTrue(t, result != nil, "expected non-nil result")

	// Verify both keys were written to memtable
	val1, found1 := mem.Get([]byte("key1"))
	tst.AssertTrue(t, found1, "expected key1 to be in memtable")
	tst.RequireDeepEqual(t, val1, []byte("value1"))

	val2, found2 := mem.Get([]byte("key2"))
	tst.AssertTrue(t, found2, "expected key2 to be in memtable")
	tst.RequireDeepEqual(t, val2, []byte("value2"))
}

// TestReplayDeleteOperation tests replay with delete operation
func TestReplayDeleteOperation(t *testing.T) {
	// Encode begin transaction
	beginPayload, err := record.EncodeBeginTxnPayload(1)
	tst.RequireNoError(t, err)
	beginFrame, err := record.EncodeFrame(record.RecordTypeBeginTransaction, beginPayload)
	tst.RequireNoError(t, err)

	// Encode delete operation
	delPayload, err := record.EncodeDeleteOpPayload(1, []byte("key1"))
	tst.RequireNoError(t, err)
	delFrame, err := record.EncodeFrame(record.RecordTypeDeleteOperation, delPayload)
	tst.RequireNoError(t, err)

	// Encode commit transaction
	commitPayload, err := record.EncodeCommitTxnPayload(1)
	tst.RequireNoError(t, err)
	commitFrame, err := record.EncodeFrame(record.RecordTypeCommitTransaction, commitPayload)
	tst.RequireNoError(t, err)

	provider := testutil.NewSegmentProvider()
	provider.AddSegment(1, append(append(beginFrame, delFrame...), commitFrame...))

	mem := memtable.New()
	start := wal.Boundary{SegId: 1, Offset: 0}
	result, err := recovery.Replay(provider, start, mem)

	tst.RequireNoError(t, err)
	tst.AssertTrue(t, result != nil, "expected non-nil result")

	// Verify the key was marked as deleted (not found)
	_, found := mem.Get([]byte("key1"))
	tst.AssertTrue(t, !found, "expected key1 to be deleted")
}

// TestReplayMultipleSegments tests replay across multiple segments
func TestReplayMultipleSegments(t *testing.T) {
	// Segment 1: transaction 1
	beginPayload1, err := record.EncodeBeginTxnPayload(1)
	tst.RequireNoError(t, err)
	beginFrame1, err := record.EncodeFrame(record.RecordTypeBeginTransaction, beginPayload1)
	tst.RequireNoError(t, err)

	putPayload1, err := record.EncodePutOpPayload(1, []byte("key1"), []byte("value1"))
	tst.RequireNoError(t, err)
	putFrame1, err := record.EncodeFrame(record.RecordTypePutOperation, putPayload1)
	tst.RequireNoError(t, err)

	commitPayload1, err := record.EncodeCommitTxnPayload(1)
	tst.RequireNoError(t, err)
	commitFrame1, err := record.EncodeFrame(record.RecordTypeCommitTransaction, commitPayload1)
	tst.RequireNoError(t, err)

	// Segment 2: transaction 2
	beginPayload2, err := record.EncodeBeginTxnPayload(2)
	tst.RequireNoError(t, err)
	beginFrame2, err := record.EncodeFrame(record.RecordTypeBeginTransaction, beginPayload2)
	tst.RequireNoError(t, err)

	putPayload2, err := record.EncodePutOpPayload(2, []byte("key2"), []byte("value2"))
	tst.RequireNoError(t, err)
	putFrame2, err := record.EncodeFrame(record.RecordTypePutOperation, putPayload2)
	tst.RequireNoError(t, err)

	commitPayload2, err := record.EncodeCommitTxnPayload(2)
	tst.RequireNoError(t, err)
	commitFrame2, err := record.EncodeFrame(record.RecordTypeCommitTransaction, commitPayload2)
	tst.RequireNoError(t, err)

	provider := testutil.NewSegmentProvider()
	provider.AddSegment(1, append(append(beginFrame1, putFrame1...), commitFrame1...))
	provider.AddSegment(2, append(append(beginFrame2, putFrame2...), commitFrame2...))

	mem := memtable.New()
	start := wal.Boundary{SegId: 1, Offset: 0}
	result, err := recovery.Replay(provider, start, mem)

	tst.RequireNoError(t, err)
	tst.AssertTrue(t, result != nil, "expected non-nil result")
	tst.AssertTrue(t, result.NextTxnId == 3, "expected NextTxnId to be 3")

	// Verify both keys were written
	val1, found1 := mem.Get([]byte("key1"))
	tst.AssertTrue(t, found1, "expected key1 to be in memtable")
	tst.RequireDeepEqual(t, val1, []byte("value1"))

	val2, found2 := mem.Get([]byte("key2"))
	tst.AssertTrue(t, found2, "expected key2 to be in memtable")
	tst.RequireDeepEqual(t, val2, []byte("value2"))
}

// TestReplayStartOffset tests replay with non-zero start offset
func TestReplayStartOffset(t *testing.T) {
	// Encode first transaction
	beginPayload1, err := record.EncodeBeginTxnPayload(1)
	tst.RequireNoError(t, err)
	beginFrame1, err := record.EncodeFrame(record.RecordTypeBeginTransaction, beginPayload1)
	tst.RequireNoError(t, err)

	putPayload1, err := record.EncodePutOpPayload(1, []byte("key1"), []byte("value1"))
	tst.RequireNoError(t, err)
	putFrame1, err := record.EncodeFrame(record.RecordTypePutOperation, putPayload1)
	tst.RequireNoError(t, err)

	commitPayload1, err := record.EncodeCommitTxnPayload(1)
	tst.RequireNoError(t, err)
	commitFrame1, err := record.EncodeFrame(record.RecordTypeCommitTransaction, commitPayload1)
	tst.RequireNoError(t, err)

	// Encode second transaction
	beginPayload2, err := record.EncodeBeginTxnPayload(2)
	tst.RequireNoError(t, err)
	beginFrame2, err := record.EncodeFrame(record.RecordTypeBeginTransaction, beginPayload2)
	tst.RequireNoError(t, err)

	putPayload2, err := record.EncodePutOpPayload(2, []byte("key2"), []byte("value2"))
	tst.RequireNoError(t, err)
	putFrame2, err := record.EncodeFrame(record.RecordTypePutOperation, putPayload2)
	tst.RequireNoError(t, err)

	commitPayload2, err := record.EncodeCommitTxnPayload(2)
	tst.RequireNoError(t, err)
	commitFrame2, err := record.EncodeFrame(record.RecordTypeCommitTransaction, commitPayload2)
	tst.RequireNoError(t, err)

	// Combine all frames
	segment1Data := append(append(beginFrame1, putFrame1...), commitFrame1...)
	offset := int64(len(segment1Data))
	allData := append(segment1Data, append(append(beginFrame2, putFrame2...), commitFrame2...)...)

	provider := testutil.NewSegmentProvider()
	provider.AddSegment(1, allData)

	mem := memtable.New()
	// Start at the second transaction
	start := wal.Boundary{SegId: 1, Offset: offset}
	result, err := recovery.Replay(provider, start, mem)

	tst.RequireNoError(t, err)
	tst.AssertTrue(t, result != nil, "expected non-nil result")

	// Only the second key should be present
	_, found1 := mem.Get([]byte("key1"))
	tst.AssertTrue(t, !found1, "expected key1 to NOT be in memtable")

	val2, found2 := mem.Get([]byte("key2"))
	tst.AssertTrue(t, found2, "expected key2 to be in memtable")
	tst.RequireDeepEqual(t, val2, []byte("value2"))
}

// TestReplayInvalidRecordData tests replay with invalid record data
func TestReplayInvalidRecordData(t *testing.T) {
	provider := testutil.NewSegmentProvider()
	// Add invalid record data (just random bytes)
	provider.AddSegment(1, []byte{0xFF, 0xFE, 0xFD})

	mem := memtable.New()
	start := wal.Boundary{SegId: 1, Offset: 0}
	_, err := recovery.Replay(provider, start, mem)

	// Should error on invalid record
	tst.AssertTrue(t, err != nil, "expected error on invalid record")
}

// TestReplayLargeValue tests replay with large values
func TestReplayLargeValue(t *testing.T) {
	largeValue := make([]byte, 100*1024) // 100KB
	for i := range largeValue {
		largeValue[i] = byte(i % 256)
	}

	// Encode transaction with large value
	beginPayload, err := record.EncodeBeginTxnPayload(1)
	tst.RequireNoError(t, err)
	beginFrame, err := record.EncodeFrame(record.RecordTypeBeginTransaction, beginPayload)
	tst.RequireNoError(t, err)

	putPayload, err := record.EncodePutOpPayload(1, []byte("largekey"), largeValue)
	tst.RequireNoError(t, err)
	putFrame, err := record.EncodeFrame(record.RecordTypePutOperation, putPayload)
	tst.RequireNoError(t, err)

	commitPayload, err := record.EncodeCommitTxnPayload(1)
	tst.RequireNoError(t, err)
	commitFrame, err := record.EncodeFrame(record.RecordTypeCommitTransaction, commitPayload)
	tst.RequireNoError(t, err)

	provider := testutil.NewSegmentProvider()
	provider.AddSegment(1, append(append(beginFrame, putFrame...), commitFrame...))

	mem := memtable.New()
	start := wal.Boundary{SegId: 1, Offset: 0}
	result, err := recovery.Replay(provider, start, mem)

	tst.RequireNoError(t, err)
	tst.AssertTrue(t, result != nil, "expected non-nil result")

	val, found := mem.Get([]byte("largekey"))
	tst.AssertTrue(t, found, "expected largekey to be in memtable")
	tst.RequireDeepEqual(t, val, largeValue)
}

// TestReplayMultipleTransactions tests replay with multiple transactions
func TestReplayMultipleTransactions(t *testing.T) {
	var allFrames []byte

	// Create 3 transactions
	for txnID := uint64(1); txnID <= 3; txnID++ {
		beginPayload, err := record.EncodeBeginTxnPayload(txnID)
		tst.RequireNoError(t, err)
		beginFrame, err := record.EncodeFrame(record.RecordTypeBeginTransaction, beginPayload)
		tst.RequireNoError(t, err)
		allFrames = append(allFrames, beginFrame...)

		key := []byte("key" + string(rune(txnID)))
		value := []byte("value" + string(rune(txnID)))
		putPayload, err := record.EncodePutOpPayload(txnID, key, value)
		tst.RequireNoError(t, err)
		putFrame, err := record.EncodeFrame(record.RecordTypePutOperation, putPayload)
		tst.RequireNoError(t, err)
		allFrames = append(allFrames, putFrame...)

		commitPayload, err := record.EncodeCommitTxnPayload(txnID)
		tst.RequireNoError(t, err)
		commitFrame, err := record.EncodeFrame(record.RecordTypeCommitTransaction, commitPayload)
		tst.RequireNoError(t, err)
		allFrames = append(allFrames, commitFrame...)
	}

	provider := testutil.NewSegmentProvider()
	provider.AddSegment(1, allFrames)

	mem := memtable.New()
	start := wal.Boundary{SegId: 1, Offset: 0}
	result, err := recovery.Replay(provider, start, mem)

	tst.RequireNoError(t, err)
	tst.AssertTrue(t, result != nil, "expected non-nil result")
	tst.AssertTrue(t, result.NextTxnId == 4, "expected NextTxnId to be 4")

	// Verify all keys are present
	for txnID := uint64(1); txnID <= 3; txnID++ {
		key := []byte("key" + string(rune(txnID)))
		value := []byte("value" + string(rune(txnID)))
		val, found := mem.Get(key)
		tst.AssertTrue(t, found, "expected key to be in memtable")
		tst.RequireDeepEqual(t, val, value)
	}
}
