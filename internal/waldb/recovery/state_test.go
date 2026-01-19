package recovery

import (
	"testing"

	tst "github.com/julianstephens/go-utils/tests"
	"github.com/julianstephens/waldb/internal/waldb/kv"
	"github.com/julianstephens/waldb/internal/waldb/memtable"
	"github.com/julianstephens/waldb/internal/waldb/wal/record"
)

// TestReplayState_ValidStateTransitions tests Absent -> Open -> Committed transition
func TestReplayState_ValidStateTransitions(t *testing.T) {
	mem := memtable.New()
	rs := NewReplayState(mem)

	// Test valid transition: Absent -> Open (via BEGIN)
	beginPayload := record.BeginCommitTransactionPayload{TxnID: 1}
	err := rs.OnBegin(beginPayload)
	tst.AssertNoError(t, err, "BEGIN should succeed for absent transaction")
	tst.AssertEqual(t, rs.MaxCommitted(), uint64(0), "maxCommitted should not change on BEGIN")

	// Test valid transition: Open -> Committed (via COMMIT)
	commitPayload := record.BeginCommitTransactionPayload{TxnID: 1}
	err = rs.OnCommit(commitPayload)
	tst.AssertNoError(t, err, "COMMIT should succeed for open transaction")
	tst.AssertEqual(t, rs.MaxCommitted(), uint64(1), "maxCommitted should update on COMMIT")
}

// TestReplayState_BeginAbsentState tests BEGIN transitions Absent -> Open
func TestReplayState_BeginAbsentState(t *testing.T) {
	mem := memtable.New()
	rs := NewReplayState(mem)

	// BEGIN on absent state should succeed
	payload := record.BeginCommitTransactionPayload{TxnID: 1}
	err := rs.OnBegin(payload)
	tst.AssertNoError(t, err, "BEGIN on absent state should succeed")

	inflight := rs.Inflight()
	tst.AssertEqual(t, len(inflight), 1, "inflight should have one transaction")
}

// TestReplayState_BeginOpenState_DuplicateBegin tests BEGIN Open -> error
func TestReplayState_BeginOpenState_DuplicateBegin(t *testing.T) {
	mem := memtable.New()
	rs := NewReplayState(mem)

	// First BEGIN
	payload := record.BeginCommitTransactionPayload{TxnID: 1}
	err := rs.OnBegin(payload)
	tst.AssertNoError(t, err, "first BEGIN should succeed")

	// Second BEGIN with same TxnID should fail
	err = rs.OnBegin(payload)
	tst.AssertTrue(t, err != nil, "duplicate BEGIN should error")
	tst.AssertEqual(t, err.(*StateError).Kind, StateDoubleBegin, "error should be StateDoubleBegin")
}

// TestReplayState_BeginCommittedState_Reuse tests BEGIN Committed -> error
func TestReplayState_BeginCommittedState_Reuse(t *testing.T) {
	mem := memtable.New()
	rs := NewReplayState(mem)

	// BEGIN and COMMIT transaction 1
	payload1 := record.BeginCommitTransactionPayload{TxnID: 1}
	err := rs.OnBegin(payload1)
	tst.AssertNoError(t, err)

	err = rs.OnCommit(payload1)
	tst.AssertNoError(t, err)

	// Try to BEGIN again with same TxnID
	err = rs.OnBegin(payload1)
	tst.AssertTrue(t, err != nil, "BEGIN after COMMIT should error")
	tst.AssertEqual(t, err.(*StateError).Kind, StateAfterCommit, "error should be StateAfterCommit")
}

// TestReplayState_PutOpenState tests PUT valid only in Open state
func TestReplayState_PutOpenState(t *testing.T) {
	mem := memtable.New()
	rs := NewReplayState(mem)

	// BEGIN transaction
	beginPayload := record.BeginCommitTransactionPayload{TxnID: 1}
	err := rs.OnBegin(beginPayload)
	tst.AssertNoError(t, err)

	// PUT in open state should succeed
	putPayload := record.PutOpPayload{
		TxnID: 1,
		Key:   []byte("key1"),
		Value: []byte("value1"),
	}
	err = rs.OnPut(putPayload)
	tst.AssertNoError(t, err, "PUT in open state should succeed")

	inflight := rs.Inflight()
	tst.AssertEqual(t, len(inflight[1].Ops), 1, "inflight transaction should have one operation")
	tst.AssertEqual(t, inflight[1].Ops[0].Kind, kv.OpPut, "operation kind should be OpPut")
}

// TestReplayState_PutAbsentState_OrphanOp tests PUT Absent -> error
func TestReplayState_PutAbsentState_OrphanOp(t *testing.T) {
	mem := memtable.New()
	rs := NewReplayState(mem)

	// PUT without BEGIN should error
	putPayload := record.PutOpPayload{
		TxnID: 1,
		Key:   []byte("key1"),
		Value: []byte("value1"),
	}
	err := rs.OnPut(putPayload)
	tst.AssertTrue(t, err != nil, "PUT on absent transaction should error")
	tst.AssertEqual(t, err.(*StateError).Kind, StateOrphanOp, "error should be StateOrphanOp")
}

// TestReplayState_PutCommittedState_OrphanOp tests PUT Committed -> error
func TestReplayState_PutCommittedState_OrphanOp(t *testing.T) {
	mem := memtable.New()
	rs := NewReplayState(mem)

	// BEGIN and COMMIT
	beginPayload := record.BeginCommitTransactionPayload{TxnID: 1}
	err := rs.OnBegin(beginPayload)
	tst.AssertNoError(t, err)

	commitPayload := record.BeginCommitTransactionPayload{TxnID: 1}
	err = rs.OnCommit(commitPayload)
	tst.AssertNoError(t, err)

	// PUT after COMMIT should error
	putPayload := record.PutOpPayload{
		TxnID: 1,
		Key:   []byte("key1"),
		Value: []byte("value1"),
	}
	err = rs.OnPut(putPayload)
	tst.AssertTrue(t, err != nil, "PUT after COMMIT should error")
	// After COMMIT, txn is removed from inflight, so it returns StateOrphanOp
	tst.AssertEqual(t, err.(*StateError).Kind, StateOrphanOp, "error should be StateOrphanOp")
}

// TestReplayState_DelOpenState tests DEL valid only in Open state
func TestReplayState_DelOpenState(t *testing.T) {
	mem := memtable.New()
	rs := NewReplayState(mem)

	// BEGIN transaction
	beginPayload := record.BeginCommitTransactionPayload{TxnID: 1}
	err := rs.OnBegin(beginPayload)
	tst.AssertNoError(t, err)

	// DEL in open state should succeed
	delPayload := record.DeleteOpPayload{
		TxnID: 1,
		Key:   []byte("key1"),
	}
	err = rs.OnDel(delPayload)
	tst.AssertNoError(t, err, "DEL in open state should succeed")

	inflight := rs.Inflight()
	tst.AssertEqual(t, len(inflight[1].Ops), 1, "inflight transaction should have one operation")
	tst.AssertEqual(t, inflight[1].Ops[0].Kind, kv.OpDelete, "operation kind should be OpDelete")
}

// TestReplayState_DelAbsentState_OrphanOp tests DEL Absent -> error
func TestReplayState_DelAbsentState_OrphanOp(t *testing.T) {
	mem := memtable.New()
	rs := NewReplayState(mem)

	// DEL without BEGIN should error
	delPayload := record.DeleteOpPayload{
		TxnID: 1,
		Key:   []byte("key1"),
	}
	err := rs.OnDel(delPayload)
	tst.AssertTrue(t, err != nil, "DEL on absent transaction should error")
	tst.AssertEqual(t, err.(*StateError).Kind, StateOrphanOp, "error should be StateOrphanOp")
}

// TestReplayState_DelCommittedState_OrphanOp tests DEL Committed -> error
func TestReplayState_DelCommittedState_OrphanOp(t *testing.T) {
	mem := memtable.New()
	rs := NewReplayState(mem)

	// BEGIN and COMMIT
	beginPayload := record.BeginCommitTransactionPayload{TxnID: 1}
	err := rs.OnBegin(beginPayload)
	tst.AssertNoError(t, err)

	commitPayload := record.BeginCommitTransactionPayload{TxnID: 1}
	err = rs.OnCommit(commitPayload)
	tst.AssertNoError(t, err)

	// DEL after COMMIT should error
	delPayload := record.DeleteOpPayload{
		TxnID: 1,
		Key:   []byte("key1"),
	}
	err = rs.OnDel(delPayload)
	tst.AssertTrue(t, err != nil, "DEL after COMMIT should error")
	// After COMMIT, txn is removed from inflight, so it returns StateOrphanOp
	tst.AssertEqual(t, err.(*StateError).Kind, StateOrphanOp, "error should be StateOrphanOp")
}

// TestReplayState_CommitOpenState tests COMMIT Open -> apply and mark Committed
func TestReplayState_CommitOpenState(t *testing.T) {
	mem := memtable.New()
	rs := NewReplayState(mem)

	// BEGIN transaction
	beginPayload := record.BeginCommitTransactionPayload{TxnID: 1}
	err := rs.OnBegin(beginPayload)
	tst.AssertNoError(t, err)

	// Add some operations
	putPayload := record.PutOpPayload{
		TxnID: 1,
		Key:   []byte("key1"),
		Value: []byte("value1"),
	}
	err = rs.OnPut(putPayload)
	tst.AssertNoError(t, err)

	// COMMIT should succeed
	commitPayload := record.BeginCommitTransactionPayload{TxnID: 1}
	err = rs.OnCommit(commitPayload)
	tst.AssertNoError(t, err, "COMMIT on open state should succeed")

	// Inflight should be empty
	inflight := rs.Inflight()
	tst.AssertEqual(t, len(inflight), 0, "inflight should be empty after COMMIT")

	// Verify the operation was applied to memtable
	val, found := mem.Get([]byte("key1"))
	tst.AssertTrue(t, found, "key1 should exist in memtable")
	tst.AssertDeepEqual(t, val, []byte("value1"), "value should be correct in memtable")
}

// TestReplayState_CommitAbsentState_Corruption tests COMMIT Absent -> error
func TestReplayState_CommitAbsentState_Corruption(t *testing.T) {
	mem := memtable.New()
	rs := NewReplayState(mem)

	// COMMIT without BEGIN should error
	commitPayload := record.BeginCommitTransactionPayload{TxnID: 1}
	err := rs.OnCommit(commitPayload)
	tst.AssertTrue(t, err != nil, "COMMIT on absent transaction should error")
	tst.AssertEqual(t, err.(*StateError).Kind, StateCommitNoTxn, "error should be StateCommitNoTxn")
}

// TestReplayState_CommitCommittedState_DoubleCommit tests COMMIT Committed -> error
func TestReplayState_CommitCommittedState_DoubleCommit(t *testing.T) {
	mem := memtable.New()
	rs := NewReplayState(mem)

	// BEGIN and first COMMIT
	beginPayload := record.BeginCommitTransactionPayload{TxnID: 1}
	err := rs.OnBegin(beginPayload)
	tst.AssertNoError(t, err)

	commitPayload := record.BeginCommitTransactionPayload{TxnID: 1}
	err = rs.OnCommit(commitPayload)
	tst.AssertNoError(t, err)

	// Second COMMIT should error
	err = rs.OnCommit(commitPayload)
	tst.AssertTrue(t, err != nil, "double COMMIT should error")
	tst.AssertEqual(t, err.(*StateError).Kind, StateCommitNoTxn, "error should be StateCommitNoTxn")
}

// TestReplayState_IgnoredState_BeginWithoutCommit tests Ignored state (BEGIN but no COMMIT)
func TestReplayState_IgnoredState_BeginWithoutCommit(t *testing.T) {
	mem := memtable.New()
	rs := NewReplayState(mem)

	// BEGIN transaction but never COMMIT
	beginPayload := record.BeginCommitTransactionPayload{TxnID: 1}
	err := rs.OnBegin(beginPayload)
	tst.AssertNoError(t, err)

	// Add operations
	putPayload := record.PutOpPayload{
		TxnID: 1,
		Key:   []byte("key1"),
		Value: []byte("value1"),
	}
	err = rs.OnPut(putPayload)
	tst.AssertNoError(t, err)

	// End of replay - transaction is ignored (never applied to memtable)
	inflight := rs.Inflight()
	tst.AssertEqual(t, len(inflight), 1, "transaction should still be inflight")

	// Verify the operation was NOT applied to memtable
	_, found := mem.Get([]byte("key1"))
	tst.AssertFalse(t, found, "key1 should NOT exist in memtable (transaction ignored)")
}

// TestReplayState_MonotonicCommitIDs tests monotonic transaction ID requirement
func TestReplayState_MonotonicCommitIDs(t *testing.T) {
	mem := memtable.New()
	rs := NewReplayState(mem)

	// Commit transaction 1
	payload1 := record.BeginCommitTransactionPayload{TxnID: 1}
	err := rs.OnBegin(payload1)
	tst.AssertNoError(t, err)

	err = rs.OnCommit(payload1)
	tst.AssertNoError(t, err)
	tst.AssertEqual(t, rs.MaxCommitted(), uint64(1))

	// Commit transaction 2
	payload2 := record.BeginCommitTransactionPayload{TxnID: 2}
	err = rs.OnBegin(payload2)
	tst.AssertNoError(t, err)

	err = rs.OnCommit(payload2)
	tst.AssertNoError(t, err)
	tst.AssertEqual(t, rs.MaxCommitted(), uint64(2))

	// Try to commit transaction 3 (should work - still monotonic)
	payload3 := record.BeginCommitTransactionPayload{TxnID: 3}
	err = rs.OnBegin(payload3)
	tst.AssertNoError(t, err)

	err = rs.OnCommit(payload3)
	tst.AssertNoError(t, err)
	tst.AssertEqual(t, rs.MaxCommitted(), uint64(3))

	// Try to commit old transaction 1 again - should error (non-monotonic)
	payload1Again := record.BeginCommitTransactionPayload{TxnID: 1}
	err = rs.OnBegin(payload1Again)
	// BEGIN should fail because txn 1 is already committed
	tst.AssertTrue(t, err != nil, "BEGIN after COMMIT should error")
}

// TestReplayState_BeginWithZeroTxnID tests BEGIN with TxnID=0 is invalid
func TestReplayState_BeginWithZeroTxnID(t *testing.T) {
	mem := memtable.New()
	rs := NewReplayState(mem)

	// BEGIN with TxnID 0 should error
	payload := record.BeginCommitTransactionPayload{TxnID: 0}
	err := rs.OnBegin(payload)
	tst.AssertTrue(t, err != nil, "BEGIN with TxnID=0 should error")
	tst.AssertEqual(t, err.(*StateError).Kind, StateTxnMismatch)
}

// TestReplayState_MultipleTransactionsInterleaved tests interleaved transactions
func TestReplayState_MultipleTransactionsInterleaved(t *testing.T) {
	mem := memtable.New()
	rs := NewReplayState(mem)

	// Begin txn 1
	payload1Begin := record.BeginCommitTransactionPayload{TxnID: 1}
	err := rs.OnBegin(payload1Begin)
	tst.AssertNoError(t, err)

	// Begin txn 2
	payload2Begin := record.BeginCommitTransactionPayload{TxnID: 2}
	err = rs.OnBegin(payload2Begin)
	tst.AssertNoError(t, err)

	// Put in txn 1
	putPayload1 := record.PutOpPayload{TxnID: 1, Key: []byte("k1"), Value: []byte("v1")}
	err = rs.OnPut(putPayload1)
	tst.AssertNoError(t, err)

	// Put in txn 2
	putPayload2 := record.PutOpPayload{TxnID: 2, Key: []byte("k2"), Value: []byte("v2")}
	err = rs.OnPut(putPayload2)
	tst.AssertNoError(t, err)

	// Commit txn 1
	payload1Commit := record.BeginCommitTransactionPayload{TxnID: 1}
	err = rs.OnCommit(payload1Commit)
	tst.AssertNoError(t, err)

	// Commit txn 2
	payload2Commit := record.BeginCommitTransactionPayload{TxnID: 2}
	err = rs.OnCommit(payload2Commit)
	tst.AssertNoError(t, err)

	// Verify both operations in memtable
	val1, found1 := mem.Get([]byte("k1"))
	tst.AssertTrue(t, found1, "k1 should exist")
	tst.AssertDeepEqual(t, val1, []byte("v1"))

	val2, found2 := mem.Get([]byte("k2"))
	tst.AssertTrue(t, found2, "k2 should exist")
	tst.AssertDeepEqual(t, val2, []byte("v2"))

	inflight := rs.Inflight()
	tst.AssertEqual(t, len(inflight), 0, "no transactions should be inflight")
	tst.AssertEqual(t, rs.MaxCommitted(), uint64(2))
}

// TestReplayState_DeleteAndPutSameKey tests delete followed by put on same key
func TestReplayState_DeleteAndPutSameKey(t *testing.T) {
	mem := memtable.New()
	rs := NewReplayState(mem)

	// Pre-populate memtable
	err := mem.Put([]byte("key1"), []byte("initial"))
	tst.AssertNoError(t, err)

	// Begin transaction
	beginPayload := record.BeginCommitTransactionPayload{TxnID: 1}
	err = rs.OnBegin(beginPayload)
	tst.AssertNoError(t, err)

	// Delete key
	delPayload := record.DeleteOpPayload{TxnID: 1, Key: []byte("key1")}
	err = rs.OnDel(delPayload)
	tst.AssertNoError(t, err)

	// Put same key with new value
	putPayload := record.PutOpPayload{
		TxnID: 1,
		Key:   []byte("key1"),
		Value: []byte("updated"),
	}
	err = rs.OnPut(putPayload)
	tst.AssertNoError(t, err)

	// Commit
	commitPayload := record.BeginCommitTransactionPayload{TxnID: 1}
	err = rs.OnCommit(commitPayload)
	tst.AssertNoError(t, err)

	// Verify final value
	val, found := mem.Get([]byte("key1"))
	tst.AssertTrue(t, found, "key1 should exist")
	tst.AssertDeepEqual(t, val, []byte("updated"))
}

// TestReplayState_CommitEmptyTransaction tests committing a transaction with no operations
func TestReplayState_CommitEmptyTransaction(t *testing.T) {
	mem := memtable.New()
	rs := NewReplayState(mem)

	// Begin transaction with no operations
	beginPayload := record.BeginCommitTransactionPayload{TxnID: 1}
	err := rs.OnBegin(beginPayload)
	tst.AssertNoError(t, err)

	// Commit empty transaction
	commitPayload := record.BeginCommitTransactionPayload{TxnID: 1}
	err = rs.OnCommit(commitPayload)
	tst.AssertNoError(t, err, "committing empty transaction should succeed")

	inflight := rs.Inflight()
	tst.AssertEqual(t, len(inflight), 0, "inflight should be empty")
	tst.AssertEqual(t, rs.MaxCommitted(), uint64(1))
}
