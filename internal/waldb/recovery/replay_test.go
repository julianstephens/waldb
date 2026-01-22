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

// TestReplayProviderErrors_TableDriven tests various provider/source errors
func TestReplayProviderErrors_TableDriven(t *testing.T) {
	testCases := []struct {
		name           string
		setupProvider  func() *testutil.SegmentProvider
		startSegID     uint64
		expectedError  bool
		expectedStatus recovery.TailStatus
	}{
		{
			name: "EmptyProvider",
			setupProvider: func() *testutil.SegmentProvider {
				return testutil.NewSegmentProvider()
			},
			startSegID:     1,
			expectedError:  true,
			expectedStatus: recovery.TailStatusMissing,
		},
		{
			name: "SegmentNotFound",
			setupProvider: func() *testutil.SegmentProvider {
				provider := testutil.NewSegmentProvider()
				provider.AddSegment(1, []byte{})
				provider.AddSegment(2, []byte{})
				return provider
			},
			startSegID:     999,
			expectedError:  true,
			expectedStatus: recovery.TailStatusMissing,
		},
		{
			name: "OpenSegmentError",
			setupProvider: func() *testutil.SegmentProvider {
				provider := testutil.NewSegmentProvider()
				provider.AddSegment(1, []byte{})
				provider.SetOpenError(errors.New("open failed"))
				return provider
			},
			startSegID:     1,
			expectedError:  true,
			expectedStatus: recovery.TailStatusCorrupt,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			provider := tc.setupProvider()
			mem := memtable.New()
			start := wal.Boundary{SegId: tc.startSegID, Offset: 0}

			result, err := recovery.Replay(provider, start, mem, logger.NoOpLogger{})

			if (err != nil) != tc.expectedError {
				t.Fatalf("expected error=%v, got=%v", tc.expectedError, err != nil)
			}
			if result == nil {
				t.Fatal("expected non-nil result")
			}
			if result.TailStatus != tc.expectedStatus {
				t.Fatalf("expected TailStatus=%v, got=%v", tc.expectedStatus, result.TailStatus)
			}
		})
	}
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

// TestReplayBasicOperations_TableDriven tests various basic operations in a single transaction
func TestReplayBasicOperations_TableDriven(t *testing.T) {
	testCases := []struct {
		name string
		ops  []struct {
			opType string // "put" or "delete"
			key    []byte
			value  []byte
		}
		expectedKeys map[string]bool // key -> shouldExist
	}{
		{
			name: "SinglePutOperation",
			ops: []struct {
				opType string
				key    []byte
				value  []byte
			}{
				{opType: "put", key: []byte("key1"), value: []byte("value1")},
			},
			expectedKeys: map[string]bool{"key1": true},
		},
		{
			name: "MultiplePutOperations",
			ops: []struct {
				opType string
				key    []byte
				value  []byte
			}{
				{opType: "put", key: []byte("key1"), value: []byte("value1")},
				{opType: "put", key: []byte("key2"), value: []byte("value2")},
			},
			expectedKeys: map[string]bool{"key1": true, "key2": true},
		},
		{
			name: "DeleteOperation",
			ops: []struct {
				opType string
				key    []byte
				value  []byte
			}{
				{opType: "delete", key: []byte("key1")},
			},
			expectedKeys: map[string]bool{"key1": false},
		},
		{
			name: "PutThenDelete",
			ops: []struct {
				opType string
				key    []byte
				value  []byte
			}{
				{opType: "put", key: []byte("key1"), value: []byte("value1")},
				{opType: "delete", key: []byte("key1")},
			},
			expectedKeys: map[string]bool{"key1": false},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Encode begin transaction
			beginPayload, err := record.EncodeBeginTxnPayload(1)
			tst.RequireNoError(t, err)
			beginFrame, err := record.EncodeFrame(record.RecordTypeBeginTransaction, beginPayload)
			tst.RequireNoError(t, err)

			// Encode operations
			var segmentData []byte = beginFrame
			for _, op := range tc.ops {
				switch op.opType {
				case "put":
					putPayload, err := record.EncodePutOpPayload(1, op.key, op.value)
					tst.RequireNoError(t, err)
					putFrame, err := record.EncodeFrame(record.RecordTypePutOperation, putPayload)
					tst.RequireNoError(t, err)
					segmentData = append(segmentData, putFrame...)
				case "delete":
					delPayload, err := record.EncodeDeleteOpPayload(1, op.key)
					tst.RequireNoError(t, err)
					delFrame, err := record.EncodeFrame(record.RecordTypeDeleteOperation, delPayload)
					tst.RequireNoError(t, err)
					segmentData = append(segmentData, delFrame...)
				}
			}

			// Encode commit transaction
			commitPayload, err := record.EncodeCommitTxnPayload(1)
			tst.RequireNoError(t, err)
			commitFrame, err := record.EncodeFrame(record.RecordTypeCommitTransaction, commitPayload)
			tst.RequireNoError(t, err)
			segmentData = append(segmentData, commitFrame...)

			provider := testutil.NewSegmentProvider()
			provider.AddSegment(1, segmentData)

			mem := memtable.New()
			start := wal.Boundary{SegId: 1, Offset: 0}
			result, err := recovery.Replay(provider, start, mem, logger.NoOpLogger{})

			tst.RequireNoError(t, err)
			tst.AssertTrue(t, result != nil, "expected non-nil result")

			// Verify expected keys
			for key, shouldExist := range tc.expectedKeys {
				val, found := mem.Get([]byte(key))
				if shouldExist {
					if !found {
						t.Fatalf("expected %s to be in memtable", key)
					}
					if found && len(tc.ops) > 0 {
						// Check value matches what was put (if put was the last operation)
						lastOp := tc.ops[len(tc.ops)-1]
						if lastOp.opType == "put" && string(lastOp.key) == key {
							tst.RequireDeepEqual(t, val, lastOp.value)
						}
					}
				} else {
					if found {
						t.Fatalf("expected %s to NOT be in memtable", key)
					}
				}
			}
		})
	}
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

// TestReplaySegmentOrdering_TableDriven tests various segment ordering violations
func TestReplaySegmentOrdering_TableDriven(t *testing.T) {
	testCases := []struct {
		name       string
		segments   []uint64 // segment IDs to add
		startSegID uint64
		wantErr    bool
	}{
		{
			name:       "InvalidSegmentID_Zero",
			segments:   []uint64{0},
			startSegID: 0,
			wantErr:    true,
		},
		{
			name:       "NonConsecutiveIDs_Gap",
			segments:   []uint64{1, 2, 4}, // Skips 3
			startSegID: 1,
			wantErr:    true,
		},
		{
			name:       "DecreasingIDs",
			segments:   []uint64{3, 2, 1}, // Decreasing order
			startSegID: 3,
			wantErr:    true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			provider := testutil.NewSegmentProvider()
			for _, segID := range tc.segments {
				provider.AddSegment(segID, []byte{})
			}

			mem := memtable.New()
			start := wal.Boundary{SegId: tc.startSegID, Offset: 0}

			result, err := recovery.Replay(provider, start, mem, logger.NoOpLogger{})

			if (err != nil) != tc.wantErr {
				t.Fatalf("expected error=%v, got=%v", tc.wantErr, err != nil)
			}
			if result == nil {
				t.Fatal("expected non-nil result")
			}

			if tc.wantErr {
				sourceErr := &recovery.ReplaySourceError{}
				if !errors.As(err, &sourceErr) {
					t.Fatal("expected ReplaySourceError")
				}
				if sourceErr.Kind != recovery.ReplaySourceSegmentOrder {
					t.Fatalf("expected ReplaySourceSegmentOrder, got %v", sourceErr.Kind)
				}
			}
		})
	}
}

// TestReplayTailTruncation_TableDriven tests various tail truncation scenarios
func TestReplayTailTruncation_TableDriven(t *testing.T) {
	testCases := []struct {
		name                      string
		setupSegments             func(t *testing.T) ([]uint64, [][]byte) // Returns segment IDs and data
		expectedTailStatus        recovery.TailStatus
		expectedError             bool
		expectedCommittedTxnCount int
	}{
		{
			name: "FinalSegmentPartialRecord",
			setupSegments: func(t *testing.T) ([]uint64, [][]byte) {
				// Segment 1: complete transaction
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

				// Segment 2: partial frame in final segment
				beginPayload2, err := record.EncodeBeginTxnPayload(2)
				tst.RequireNoError(t, err)
				beginFrame2, err := record.EncodeFrame(record.RecordTypeBeginTransaction, beginPayload2)
				tst.RequireNoError(t, err)
				truncatedFrame2 := beginFrame2[:len(beginFrame2)/2]

				return []uint64{1, 2}, [][]byte{segment1Data, truncatedFrame2}
			},
			expectedTailStatus:        recovery.TailStatusCorrupt,
			expectedError:             true,
			expectedCommittedTxnCount: 1,
		},
		{
			name: "SuccessWithTruncatedStatus",
			setupSegments: func(t *testing.T) ([]uint64, [][]byte) {
				// Segment 1: complete transaction
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

				// Segment 2: complete transaction followed by incomplete one
				beginPayload2, err := record.EncodeBeginTxnPayload(2)
				tst.RequireNoError(t, err)
				beginFrame2, err := record.EncodeFrame(record.RecordTypeBeginTransaction, beginPayload2)
				tst.RequireNoError(t, err)

				putPayload2, err := record.EncodePutOpPayload(2, []byte("k2"), []byte("v2"))
				tst.RequireNoError(t, err)
				putFrame2, err := record.EncodeFrame(record.RecordTypePutOperation, putPayload2)
				tst.RequireNoError(t, err)

				commitPayload2, err := record.EncodeCommitTxnPayload(2)
				tst.RequireNoError(t, err)
				commitFrame2, err := record.EncodeFrame(record.RecordTypeCommitTransaction, commitPayload2)
				tst.RequireNoError(t, err)

				segment2Data := append(append(beginFrame2, putFrame2...), commitFrame2...)

				// Segment 3 (final): incomplete transaction at tail
				beginPayload3, err := record.EncodeBeginTxnPayload(3)
				tst.RequireNoError(t, err)
				beginFrame3, err := record.EncodeFrame(record.RecordTypeBeginTransaction, beginPayload3)
				tst.RequireNoError(t, err)

				putPayload3, err := record.EncodePutOpPayload(3, []byte("k3"), []byte("v3"))
				tst.RequireNoError(t, err)
				putFrame3, err := record.EncodeFrame(record.RecordTypePutOperation, putPayload3)
				tst.RequireNoError(t, err)

				commitPayload3, err := record.EncodeCommitTxnPayload(3)
				tst.RequireNoError(t, err)
				commitFrame3, err := record.EncodeFrame(record.RecordTypeCommitTransaction, commitPayload3)
				tst.RequireNoError(t, err)

				segment3Data := append(
					append(append(beginFrame3, putFrame3...), commitFrame3...),
					[]byte{0xFF, 0xFF}...)
				// Truncate partway through extra data
				truncatedSegment3 := segment3Data[:len(append(append(beginFrame3, putFrame3...), commitFrame3...))+1]

				return []uint64{1, 2, 3}, [][]byte{segment1Data, segment2Data, truncatedSegment3}
			},
			expectedTailStatus:        recovery.TailStatusTruncated,
			expectedError:             false,
			expectedCommittedTxnCount: 3,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			segmentIDs, segmentData := tc.setupSegments(t)

			provider := testutil.NewSegmentProvider()
			for i, segID := range segmentIDs {
				provider.AddSegment(segID, segmentData[i])
			}

			mem := memtable.New()
			start := wal.Boundary{SegId: segmentIDs[0], Offset: 0}
			result, err := recovery.Replay(provider, start, mem, logger.NoOpLogger{})

			if (err != nil) != tc.expectedError {
				t.Fatalf("expected error=%v, got=%v", tc.expectedError, err != nil)
			}
			if result == nil {
				t.Fatal("expected non-nil result")
			}
			if result.TailStatus != tc.expectedTailStatus {
				t.Fatalf("expected TailStatus=%v, got=%v", tc.expectedTailStatus, result.TailStatus)
			}

			// Verify committed transaction count by checking memtable keys
			committedCount := 0
			for i := 1; i <= 3; i++ {
				key := []byte("key" + string(rune(48+i)))
				switch i {
				case 2:
					key = []byte("k2")
				case 3:
					key = []byte("k3")
				}
				_, found := mem.Get(key)
				if found {
					committedCount++
				}
			}

			if committedCount != tc.expectedCommittedTxnCount {
				t.Fatalf("expected %d committed transactions, got %d", tc.expectedCommittedTxnCount, committedCount)
			}
		})
	}
}

// TestReplayMiddleCorruption_StopsImmediately tests that corruption in the middle
// of a segment stops replay immediately and returns an error
func TestReplayMiddleCorruption_StopsImmediately(t *testing.T) {
	// Create first complete transaction
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

	// Create a second transaction that we'll corrupt in the middle
	beginPayload2, err := record.EncodeBeginTxnPayload(2)
	tst.RequireNoError(t, err)
	beginFrame2, err := record.EncodeFrame(record.RecordTypeBeginTransaction, beginPayload2)
	tst.RequireNoError(t, err)

	// Create good PUT, then corrupt it
	putPayload2, err := record.EncodePutOpPayload(2, []byte("key2"), []byte("value2"))
	tst.RequireNoError(t, err)
	putFrame2, err := record.EncodeFrame(record.RecordTypePutOperation, putPayload2)
	tst.RequireNoError(t, err)

	// Corrupt the middle of putFrame2 (flip some bits)
	corruptedPutFrame2 := make([]byte, len(putFrame2))
	copy(corruptedPutFrame2, putFrame2)
	corruptedPutFrame2[len(corruptedPutFrame2)/2] ^= 0xFF // Flip bits in the middle

	// Good commit frame
	commitPayload2, err := record.EncodeCommitTxnPayload(2)
	tst.RequireNoError(t, err)
	commitFrame2, err := record.EncodeFrame(record.RecordTypeCommitTransaction, commitPayload2)
	tst.RequireNoError(t, err)

	segmentData := append(append(beginFrame1, putFrame1...), commitFrame1...)
	segmentData = append(append(append(segmentData, beginFrame2...), corruptedPutFrame2...), commitFrame2...)

	provider := testutil.NewSegmentProvider()
	provider.AddSegment(1, segmentData)

	mem := memtable.New()
	start := wal.Boundary{SegId: 1, Offset: 0}
	result, err := recovery.Replay(provider, start, mem, logger.NoOpLogger{})

	// Should get an error for the corruption
	tst.AssertTrue(t, err != nil, "expected error for middle corruption")
	tst.AssertTrue(t, result != nil, "expected non-nil result with partial replay")
	tst.AssertEqual(t, result.TailStatus, recovery.TailStatusCorrupt, "expected TailStatusCorrupt")

	// LastValidBoundary should point to end of last good record (end of first transaction's COMMIT)
	tst.AssertTrue(t, result.LastValid.SegId == 1, "LastValidBoundary should be in segment 1")
	tst.AssertTrue(t, result.LastValid.Offset > 0, "LastValidBoundary offset should be > 0")

	// Only the first transaction should have been applied
	val, found := mem.Get([]byte("key1"))
	tst.AssertTrue(t, found, "expected key1 to be in memtable from committed txn 1")
	tst.RequireDeepEqual(t, val, []byte("value1"))

	// key2 should not be in memtable (txn 2 was never committed due to corruption)
	_, found = mem.Get([]byte("key2"))
	tst.AssertFalse(t, found, "expected key2 to NOT be in memtable (txn 2 failed due to corruption)")
}

// TestReplayCorruption_LastValidBoundary tests that LastValidBoundary points to the
// end of the last successfully processed record when corruption occurs
func TestReplayCorruption_LastValidBoundary(t *testing.T) {
	// Create transaction 1 - will be fully processed
	beginPayload1, err := record.EncodeBeginTxnPayload(1)
	tst.RequireNoError(t, err)
	beginFrame1, err := record.EncodeFrame(record.RecordTypeBeginTransaction, beginPayload1)
	tst.RequireNoError(t, err)

	commitPayload1, err := record.EncodeCommitTxnPayload(1)
	tst.RequireNoError(t, err)
	commitFrame1, err := record.EncodeFrame(record.RecordTypeCommitTransaction, commitPayload1)
	tst.RequireNoError(t, err)

	// Create transaction 2 with corrupted data
	beginPayload2, err := record.EncodeBeginTxnPayload(2)
	tst.RequireNoError(t, err)
	beginFrame2, err := record.EncodeFrame(record.RecordTypeBeginTransaction, beginPayload2)
	tst.RequireNoError(t, err)

	// Create corrupted commit frame for txn 2
	corruptedCommit := []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF}

	segmentData := append(append(beginFrame1, commitFrame1...), beginFrame2...)
	segmentData = append(segmentData, corruptedCommit...)

	provider := testutil.NewSegmentProvider()
	provider.AddSegment(1, segmentData)

	mem := memtable.New()
	start := wal.Boundary{SegId: 1, Offset: 0}
	result, err := recovery.Replay(provider, start, mem, logger.NoOpLogger{})

	// Expect error due to corruption
	tst.AssertTrue(t, err != nil, "expected error for corruption")
	tst.AssertTrue(t, result != nil, "expected non-nil result")
	tst.AssertEqual(
		t,
		result.TailStatus,
		recovery.TailStatusCorrupt,
		"expected TailStatusCorrupt for mid-log corruption",
	)

	// LastValidBoundary should point after the BEGIN of txn 2 and BEFORE the corrupted commit
	// (at the start of the corrupted record)
	tst.AssertEqual(t, result.LastValid.SegId, uint64(1), "LastValidBoundary should be in segment 1")

	// The offset should be after BEGIN frame 1 and COMMIT frame 1 and BEGIN frame 2
	expectedOffset := int64(len(beginFrame1) + len(commitFrame1) + len(beginFrame2))
	tst.AssertEqual(
		t,
		result.LastValid.Offset,
		expectedOffset,
		"LastValidBoundary offset should point to start of corrupted record",
	)
}

// TestReplayDecodeError_MissingOffsetIsTreatedAsCorrupt tests that decode errors
// without offset metadata are consistently treated as corrupt (not truncated)
// in non-final segments
func TestReplayDecodeError_MissingOffsetIsTreatedAsCorrupt(t *testing.T) {
	// Create a complete transaction in segment 1
	beginPayload1, err := record.EncodeBeginTxnPayload(1)
	tst.RequireNoError(t, err)
	beginFrame1, err := record.EncodeFrame(record.RecordTypeBeginTransaction, beginPayload1)
	tst.RequireNoError(t, err)

	commitPayload1, err := record.EncodeCommitTxnPayload(1)
	tst.RequireNoError(t, err)
	commitFrame1, err := record.EncodeFrame(record.RecordTypeCommitTransaction, commitPayload1)
	tst.RequireNoError(t, err)

	segment1Data := append(beginFrame1, commitFrame1...)

	// Segment 2 has undecodable data (invalid decode in middle, not tail)
	badData := []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF}

	// Segment 3 exists (so segment 2 is not the final segment)
	goodSegment3 := []byte{0x00}

	provider := testutil.NewSegmentProvider()
	provider.AddSegment(1, segment1Data)
	provider.AddSegment(2, badData)
	provider.AddSegment(3, goodSegment3) // Non-final segment, so corruption is not recoverable

	mem := memtable.New()
	start := wal.Boundary{SegId: 1, Offset: 0}
	result, err := recovery.Replay(provider, start, mem, logger.NoOpLogger{})

	// Should get an error (decode error in non-final segment is not recoverable)
	tst.AssertTrue(t, err != nil, "expected error for decode error in non-final segment")
	tst.AssertTrue(t, result != nil, "expected non-nil result")
	// Should be treated as corrupt, not truncated
	tst.AssertEqual(
		t,
		result.TailStatus,
		recovery.TailStatusCorrupt,
		"expected TailStatusCorrupt for decode error in non-final segment",
	)
}
