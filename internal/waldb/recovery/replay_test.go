package recovery_test

import (
	"errors"
	"testing"

	tst "github.com/julianstephens/go-utils/tests"
	"github.com/julianstephens/waldb/internal/logger"
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

	result, err := recovery.Replay(provider, start, mem, logger.NoOpLogger{})
	// Empty provider means no segments, so we expect an error
	tst.AssertTrue(t, err != nil, "expected error with empty provider")
	tst.AssertTrue(t, result != nil, "expected non-nil result")
}

// TestReplaySegmentNotFound tests replay with starting segment that doesn't exist
func TestReplaySegmentNotFound(t *testing.T) {
	provider := testutil.NewSegmentProvider()
	provider.AddSegment(1, []byte{})
	provider.AddSegment(2, []byte{})

	mem := memtable.New()
	// Start at segment 999 which doesn't exist
	start := wal.Boundary{SegId: 999, Offset: 0}

	result, err := recovery.Replay(provider, start, mem, logger.NoOpLogger{})
	tst.AssertTrue(t, err != nil, "expected error when segment not found")
	tst.AssertTrue(t, result != nil, "expected non-nil result")
	tst.AssertTrue(t, result.TailStatus == recovery.TailStatusMissing, "expected TailStatusMissing")
}

// TestReplayOpenSegmentError tests replay when opening segment fails
func TestReplayOpenSegmentError(t *testing.T) {
	provider := testutil.NewSegmentProvider()
	provider.AddSegment(1, []byte{})
	provider.SetOpenError(errors.New("open failed"))

	mem := memtable.New()
	start := wal.Boundary{SegId: 1, Offset: 0}

	result, err := recovery.Replay(provider, start, mem, logger.NoOpLogger{})
	tst.AssertTrue(t, err != nil, "expected error when opening segment fails")
	tst.AssertTrue(t, result != nil, "expected non-nil result")
	tst.AssertTrue(t, result.TailStatus == recovery.TailStatusCorrupt, "expected TailStatusCorrupt")
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
	result, err := recovery.Replay(provider, start, mem, logger.NoOpLogger{})

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
	result, err := recovery.Replay(provider, start, mem, logger.NoOpLogger{})

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
	result, err := recovery.Replay(provider, start, mem, logger.NoOpLogger{})

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
	result, err := recovery.Replay(provider, start, mem, logger.NoOpLogger{})

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
	result, err := recovery.Replay(provider, start, mem, logger.NoOpLogger{})

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
	result, err := recovery.Replay(provider, start, mem, logger.NoOpLogger{})

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
	// Add an invalid record type byte - need at least a full header (4 bytes) + body + crc to trigger parse error
	// Create a record with a length that's way too large
	invalidData := []byte{0xFF, 0xFF, 0xFF, 0x7F} // Large declared length in little-endian
	provider.AddSegment(1, invalidData)

	mem := memtable.New()
	start := wal.Boundary{SegId: 1, Offset: 0}
	result, err := recovery.Replay(provider, start, mem, logger.NoOpLogger{})

	// Should error on invalid/truncated record
	tst.AssertTrue(t, err != nil, "expected error on invalid record")
	tst.AssertTrue(t, result != nil, "expected non-nil result")
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
	result, err := recovery.Replay(provider, start, mem, logger.NoOpLogger{})

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
	result, err := recovery.Replay(provider, start, mem, logger.NoOpLogger{})

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

// TestReplayTruncationAtOffset0InLastSegment tests the policy for truncation at offset 0
// in the last segment: it's treated as a clean EOF (valid tail status).
// Policy: A truncation at the exact start of a record boundary is treated as if that
// record was never written, which is safe because no partial data from that record
// made it to the memtable.
func TestReplayTruncationAtOffset0InLastSegment(t *testing.T) {
	// Create a complete transaction in segment 1
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

	segment1Data := append(append(beginFrame1, putFrame1...), commitFrame1...)

	// Segment 2 will have truncation at offset 0 (empty or immediately truncated)
	provider := testutil.NewSegmentProvider()
	provider.AddSegment(1, segment1Data)
	provider.AddSegment(2, []byte{}) // Empty segment (truncation at offset 0)

	mem := memtable.New()
	start := wal.Boundary{SegId: 1, Offset: 0}
	result, err := recovery.Replay(provider, start, mem, logger.NoOpLogger{})

	tst.RequireNoError(t, err)
	tst.AssertTrue(t, result != nil, "expected non-nil result")
	tst.AssertTrue(
		t,
		result.TailStatus == recovery.TailStatusValid,
		"expected TailStatusValid for truncation at offset 0 in last segment",
	)
	tst.AssertTrue(t, result.NextTxnId == 2, "expected NextTxnId to be 2")

	// Verify the committed transaction is present
	val, found := mem.Get([]byte("key1"))
	tst.AssertTrue(t, found, "expected key1 to be in memtable")
	tst.RequireDeepEqual(t, val, []byte("value1"))
}

// TestReplayTruncationInNonFinalSegment tests that truncation in a non-final segment
// is treated as corruption, not as a valid tail.
func TestReplayTruncationInNonFinalSegment(t *testing.T) {
	// Create a complete transaction in segment 1
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

	segment1Data := append(append(beginFrame1, putFrame1...), commitFrame1...)

	// Segment 2 has partial transaction (will cause truncation when reading)
	beginPayload2, err := record.EncodeBeginTxnPayload(2)
	tst.RequireNoError(t, err)
	beginFrame2, err := record.EncodeFrame(record.RecordTypeBeginTransaction, beginPayload2)
	tst.RequireNoError(t, err)

	// Truncate the begin frame to make it incomplete
	truncatedBeginFrame := beginFrame2[:len(beginFrame2)-1]

	// Segment 3 is present after the truncated segment 2
	beginPayload3, err := record.EncodeBeginTxnPayload(3)
	tst.RequireNoError(t, err)
	beginFrame3, err := record.EncodeFrame(record.RecordTypeBeginTransaction, beginPayload3)
	tst.RequireNoError(t, err)

	provider := testutil.NewSegmentProvider()
	provider.AddSegment(1, segment1Data)
	provider.AddSegment(2, truncatedBeginFrame)
	provider.AddSegment(3, beginFrame3) // Non-empty segment 3 after truncated segment 2

	mem := memtable.New()
	start := wal.Boundary{SegId: 1, Offset: 0}
	result, err := recovery.Replay(provider, start, mem, logger.NoOpLogger{})

	// Expect an error due to truncation in non-final segment
	tst.AssertTrue(t, err != nil, "expected error for truncation in non-final segment")
	tst.AssertTrue(t, result != nil, "expected non-nil result with partial replay")
	tst.AssertTrue(
		t,
		result.TailStatus == recovery.TailStatusCorrupt,
		"expected TailStatusCorrupt for truncation in non-final segment",
	)

	// Only the first transaction should have been applied
	val, found := mem.Get([]byte("key1"))
	tst.AssertTrue(t, found, "expected key1 to be in memtable")
	tst.RequireDeepEqual(t, val, []byte("value1"))
}

// TestReplayDecodeErrorMissingOffsetInLastSegment tests that a decode error
// without offset metadata in the last segment still results in TailStatusTruncated.
// This represents the case where we detected corruption but can't pinpoint the exact
// location, so we conservatively treat the entire unprocessed tail as corrupted.
func TestReplayDecodeErrorMissingOffsetInLastSegment(t *testing.T) {
	// Create a complete transaction in segment 1
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

	segment1Data := append(append(beginFrame1, putFrame1...), commitFrame1...)

	// Segment 2 (final segment) has invalid data that can't be parsed
	invalidData := []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF}

	provider := testutil.NewSegmentProvider()
	provider.AddSegment(1, segment1Data)
	provider.AddSegment(2, invalidData)

	mem := memtable.New()
	start := wal.Boundary{SegId: 1, Offset: 0}
	result, err := recovery.Replay(provider, start, mem, logger.NoOpLogger{})

	// Should get an error due to invalid data
	tst.AssertTrue(t, err != nil, "expected error for decode error")
	tst.AssertTrue(t, result != nil, "expected non-nil result")
	// In the last segment, a decode error with truncation-like characteristics should
	// be reported as a corrupted tail (since we can't verify the exact corruption point)
	tst.AssertTrue(
		t,
		result.TailStatus == recovery.TailStatusCorrupt,
		"expected TailStatusCorrupt for decode error in last segment",
	)

	// The committed transaction from segment 1 should still be present
	val, found := mem.Get([]byte("key1"))
	tst.AssertTrue(t, found, "expected key1 to be in memtable")
	tst.RequireDeepEqual(t, val, []byte("value1"))
}
