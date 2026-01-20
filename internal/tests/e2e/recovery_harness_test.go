package e2e_test

import (
	"testing"

	tst "github.com/julianstephens/go-utils/tests"
	"github.com/julianstephens/waldb/internal/testutil"
	"github.com/julianstephens/waldb/internal/waldb/recovery"
	"github.com/julianstephens/waldb/internal/waldb/wal"
)

// TestHarness_BasicTransaction tests a simple committed transaction.
// Sequence: BEGIN(1) PUT(1,k,v) COMMIT(1)
func TestHarness_BasicTransaction(t *testing.T) {
	harness := testutil.NewHarness(
		testutil.NewSequence().
			Begin(1).
			Put(1, []byte("key1"), []byte("value1")).
			Commit(1),
	)

	tst.RequireNoError(t, harness.BuildSegments())
	tst.RequireNoError(t, harness.Replay(wal.Boundary{SegId: 1, Offset: 0}))

	harness.AssertReplaySuccess(t)
	harness.AssertLastCommittedTxnID(t, 1)
	harness.AssertNextTxnID(t, 2)
	harness.AssertTailStatus(t, recovery.TailStatusValid)
	harness.AssertMemtableEntry(t, []byte("key1"), []byte("value1"))
}

// TestHarness_OrphanOperation tests a PUT without a transaction wrapper.
// Sequence: PUT(1,k,v)
func TestHarness_OrphanOperation(t *testing.T) {
	harness := testutil.NewHarness(
		testutil.NewSequence().
			Put(1, []byte("key1"), []byte("value1")),
	)

	tst.RequireNoError(t, harness.BuildSegments())
	err := harness.Replay(wal.Boundary{SegId: 1, Offset: 0})

	// Orphan operations should be rejected during replay
	tst.AssertNotNil(t, err, "expected replay to return an error for orphan operation")
	harness.AssertReplayError(t)
}

// TestHarness_OperationAfterCommit tests a PUT that appears after COMMIT.
// Sequence: BEGIN(1) COMMIT(1) PUT(1,k,v)
func TestHarness_OperationAfterCommit(t *testing.T) {
	harness := testutil.NewHarness(
		testutil.NewSequence().
			Begin(1).
			Commit(1).
			Put(1, []byte("key1"), []byte("value1")),
	)

	tst.RequireNoError(t, harness.BuildSegments())
	err := harness.Replay(wal.Boundary{SegId: 1, Offset: 0})

	// Operation after commit should be rejected
	tst.AssertNotNil(t, err, "expected replay to return an error for operation after commit")
	harness.AssertReplayError(t)
}

// TestHarness_IncompleteTransaction tests a transaction without COMMIT.
// Sequence: BEGIN(1) PUT(1,k,v)
func TestHarness_IncompleteTransaction(t *testing.T) {
	harness := testutil.NewHarness(
		testutil.NewSequence().
			Begin(1).
			Put(1, []byte("key1"), []byte("value1")),
	)

	tst.RequireNoError(t, harness.BuildSegments())
	tst.RequireNoError(t, harness.Replay(wal.Boundary{SegId: 1, Offset: 0}))

	harness.AssertReplaySuccess(t)
	// Incomplete transactions are not committed, so they shouldn't be in memtable
	harness.AssertMemtableEntry(t, []byte("key1"), nil)
	// And NextTxnId should not advance past the committed transactions
	harness.AssertLastCommittedTxnID(t, 0)
	harness.AssertNextTxnID(t, 1)
}

// TestHarness_DuplicateBegin tests a duplicate BEGIN without COMMIT between them.
// Sequence: BEGIN(1) BEGIN(1)
func TestHarness_DuplicateBegin(t *testing.T) {
	harness := testutil.NewHarness(
		testutil.NewSequence().
			Begin(1).
			Begin(1),
	)

	tst.RequireNoError(t, harness.BuildSegments())
	err := harness.Replay(wal.Boundary{SegId: 1, Offset: 0})

	// Duplicate BEGIN should be rejected (transaction already in progress)
	tst.AssertNotNil(t, err, "expected replay to return an error for duplicate BEGIN")
	harness.AssertReplayError(t)
}

// TestHarness_MultipleCommittedTransactions tests multiple transactions.
// Sequence: BEGIN(1) PUT(1,k1,v1) COMMIT(1) BEGIN(2) PUT(2,k2,v2) COMMIT(2)
func TestHarness_MultipleCommittedTransactions(t *testing.T) {
	harness := testutil.NewHarness(
		testutil.NewSequence().
			Begin(1).
			Put(1, []byte("k1"), []byte("v1")).
			Commit(1).
			Begin(2).
			Put(2, []byte("k2"), []byte("v2")).
			Commit(2),
	)

	tst.RequireNoError(t, harness.BuildSegments())
	tst.RequireNoError(t, harness.Replay(wal.Boundary{SegId: 1, Offset: 0}))

	harness.AssertReplaySuccess(t)
	harness.AssertLastCommittedTxnID(t, 2)
	harness.AssertNextTxnID(t, 3)
	harness.AssertMemtableEntry(t, []byte("k1"), []byte("v1"))
	harness.AssertMemtableEntry(t, []byte("k2"), []byte("v2"))
}

// TestHarness_SegmentGaps tests WAL with gaps in segment numbering.
// Segments: wal-000001.log, wal-000003.log (000002 is missing)
// Gaps in segment IDs are not allowed - segments must be consecutive.
// This test verifies that replay fails with an appropriate error.
func TestHarness_SegmentGaps(t *testing.T) {
	seq := testutil.NewSequence().
		Begin(1).
		Put(1, []byte("k1"), []byte("v1")).
		Commit(1).
		Begin(2).
		Put(2, []byte("k2"), []byte("v2")).
		Commit(2)

	harness := testutil.NewHarness(seq)

	// Build segments with explicit IDs: 1 and 3 (gap at 2)
	tst.RequireNoError(t, harness.BuildSegmentsWithGaps([]uint64{1, 3}))
	// Expect an error due to non-consecutive segment IDs
	tst.AssertTrue(
		t,
		harness.Replay(wal.Boundary{SegId: 1, Offset: 0}) != nil,
		"expected replay to fail with non-consecutive segments",
	)

	// Segments with gaps should fail validation
	harness.AssertReplayError(t)
}

// TestHarness_DeleteOperation tests DELETE operations.
// Sequence: BEGIN(1) PUT(1,k,v) COMMIT(1) BEGIN(2) DELETE(2,k) COMMIT(2)
func TestHarness_DeleteOperation(t *testing.T) {
	harness := testutil.NewHarness(
		testutil.NewSequence().
			Begin(1).
			Put(1, []byte("key1"), []byte("value1")).
			Commit(1).
			Begin(2).
			Delete(2, []byte("key1")).
			Commit(2),
	)

	tst.RequireNoError(t, harness.BuildSegments())
	tst.RequireNoError(t, harness.Replay(wal.Boundary{SegId: 1, Offset: 0}))

	harness.AssertReplaySuccess(t)
	harness.AssertLastCommittedTxnID(t, 2)
	harness.AssertNextTxnID(t, 3)
	// Key should be deleted (not present in memtable)
	harness.AssertMemtableEntry(t, []byte("key1"), nil)
}

// TestHarness_MultipleOperationsPerTransaction tests a transaction with multiple PUTs.
// Sequence: BEGIN(1) PUT(1,k1,v1) PUT(1,k2,v2) PUT(1,k3,v3) COMMIT(1)
func TestHarness_MultipleOperationsPerTransaction(t *testing.T) {
	harness := testutil.NewHarness(
		testutil.NewSequence().
			Begin(1).
			Put(1, []byte("k1"), []byte("v1")).
			Put(1, []byte("k2"), []byte("v2")).
			Put(1, []byte("k3"), []byte("v3")).
			Commit(1),
	)

	tst.RequireNoError(t, harness.BuildSegments())
	tst.RequireNoError(t, harness.Replay(wal.Boundary{SegId: 1, Offset: 0}))

	harness.AssertReplaySuccess(t)
	harness.AssertLastCommittedTxnID(t, 1)
	harness.AssertNextTxnID(t, 2)
	harness.AssertMemtableEntry(t, []byte("k1"), []byte("v1"))
	harness.AssertMemtableEntry(t, []byte("k2"), []byte("v2"))
	harness.AssertMemtableEntry(t, []byte("k3"), []byte("v3"))
}

// TestHarness_MixedIncompleteAndComplete tests mixing complete and incomplete transactions.
// Sequence:
//
//	BEGIN(1) PUT(1,k1,v1) COMMIT(1)     <- complete
//	BEGIN(2) PUT(2,k2,v2)                <- incomplete
//	BEGIN(3) PUT(3,k3,v3) COMMIT(3)     <- complete
func TestHarness_MixedIncompleteAndComplete(t *testing.T) {
	harness := testutil.NewHarness(
		testutil.NewSequence().
			Begin(1).
			Put(1, []byte("k1"), []byte("v1")).
			Commit(1).
			Begin(2).
			Put(2, []byte("k2"), []byte("v2")).
			Begin(3).
			Put(3, []byte("k3"), []byte("v3")).
			Commit(3),
	)

	tst.RequireNoError(t, harness.BuildSegments())
	tst.RequireNoError(t, harness.Replay(wal.Boundary{SegId: 1, Offset: 0}))

	harness.AssertReplaySuccess(t)
	// Only committed transactions should be counted
	harness.AssertLastCommittedTxnID(t, 3)
	harness.AssertNextTxnID(t, 4)
	// Only complete transactions' data should be in memtable
	harness.AssertMemtableEntry(t, []byte("k1"), []byte("v1"))
	harness.AssertMemtableEntry(t, []byte("k2"), nil)
	harness.AssertMemtableEntry(t, []byte("k3"), []byte("v3"))
}

// TestHarness_SequenceWithUpdates tests overwriting keys across transactions.
// Sequence:
//
//	BEGIN(1) PUT(1,k,v1) COMMIT(1)
//	BEGIN(2) PUT(2,k,v2) COMMIT(2)
//	BEGIN(3) PUT(3,k,v3) COMMIT(3)
func TestHarness_SequenceWithUpdates(t *testing.T) {
	harness := testutil.NewHarness(
		testutil.NewSequence().
			Begin(1).
			Put(1, []byte("key"), []byte("value1")).
			Commit(1).
			Begin(2).
			Put(2, []byte("key"), []byte("value2")).
			Commit(2).
			Begin(3).
			Put(3, []byte("key"), []byte("value3")).
			Commit(3),
	)

	tst.RequireNoError(t, harness.BuildSegments())
	tst.RequireNoError(t, harness.Replay(wal.Boundary{SegId: 1, Offset: 0}))

	harness.AssertReplaySuccess(t)
	harness.AssertLastCommittedTxnID(t, 3)
	harness.AssertNextTxnID(t, 4)
	// Should have the latest value
	harness.AssertMemtableEntry(t, []byte("key"), []byte("value3"))
}
