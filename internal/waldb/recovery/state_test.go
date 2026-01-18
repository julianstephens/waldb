package recovery_test

import (
	"errors"
	"testing"

	tst "github.com/julianstephens/go-utils/tests"
	"github.com/julianstephens/waldb/internal/waldb/memtable"
	"github.com/julianstephens/waldb/internal/waldb/recovery"
	"github.com/julianstephens/waldb/internal/waldb/wal/record"
)

// TestReplayStateOnBeginSuccess tests successful begin transaction
func TestReplayStateOnBeginSuccess(t *testing.T) {
	mem := memtable.New()
	state := recovery.NewReplayState(mem)

	payload := record.BeginCommitTransactionPayload{TxnID: 1}
	ctx := recovery.NewRecordCtx(0, 1, 8, record.RecordTypeBeginTransaction)

	err := state.OnBegin(payload, ctx)
	tst.RequireNoError(t, err)

	// Verify transaction is inflight
	inflight := state.Inflight()
	tst.AssertTrue(t, len(inflight) == 1, "expected 1 inflight transaction")
	_, exists := inflight[1]
	tst.AssertTrue(t, exists, "expected transaction 1 to be inflight")
}

// TestReplayStateOnBeginZeroTxnId tests begin with zero transaction ID
func TestReplayStateOnBeginZeroTxnId(t *testing.T) {
	mem := memtable.New()
	state := recovery.NewReplayState(mem)

	payload := record.BeginCommitTransactionPayload{TxnID: 0}
	ctx := recovery.NewRecordCtx(0, 1, 8, record.RecordTypeBeginTransaction)

	err := state.OnBegin(payload, ctx)
	if err == nil {
		t.Fatal("expected error for zero TxnID")
	}
	if !errors.Is(err, recovery.ErrTxnMismatch) {
		t.Errorf("expected ErrTxnMismatch, got %v", err)
	}
}

// TestReplayStateOnBeginDoubleBegin tests double begin for same transaction
func TestReplayStateOnBeginDoubleBegin(t *testing.T) {
	mem := memtable.New()
	state := recovery.NewReplayState(mem)

	payload := record.BeginCommitTransactionPayload{TxnID: 1}
	ctx := recovery.NewRecordCtx(0, 1, 8, record.RecordTypeBeginTransaction)

	// First begin should succeed
	err := state.OnBegin(payload, ctx)
	tst.RequireNoError(t, err)

	// Second begin with same ID should fail
	err = state.OnBegin(payload, ctx)
	if err == nil {
		t.Fatal("expected error on double begin")
	}
	if !errors.Is(err, recovery.ErrDoubleBegin) {
		t.Errorf("expected ErrDoubleBegin, got %v", err)
	}
}

// TestReplayStateOnPutSuccess tests successful put operation
func TestReplayStateOnPutSuccess(t *testing.T) {
	mem := memtable.New()
	state := recovery.NewReplayState(mem)

	// Begin transaction
	beginPayload := record.BeginCommitTransactionPayload{TxnID: 1}
	ctx := recovery.NewRecordCtx(0, 1, 8, record.RecordTypeBeginTransaction)
	tst.RequireNoError(t, state.OnBegin(beginPayload, ctx))

	// Add put operation
	putPayload := record.PutOpPayload{
		TxnID: 1,
		Key:   []byte("testkey"),
		Value: []byte("testvalue"),
	}
	ctx = recovery.NewRecordCtx(8, 1, 25, record.RecordTypePutOperation)

	err := state.OnPut(putPayload, ctx)
	tst.RequireNoError(t, err)

	// Verify operation is stored
	inflight := state.Inflight()
	txnBuf, exists := inflight[1]
	tst.AssertTrue(t, exists, "expected transaction 1 to be inflight")
	tst.AssertTrue(t, len(txnBuf.Ops) == 1, "expected 1 operation")
}

// TestReplayStateOnPutWithoutBegin tests put without active transaction
func TestReplayStateOnPutWithoutBegin(t *testing.T) {
	mem := memtable.New()
	state := recovery.NewReplayState(mem)

	putPayload := record.PutOpPayload{
		TxnID: 1,
		Key:   []byte("testkey"),
		Value: []byte("testvalue"),
	}
	ctx := recovery.NewRecordCtx(0, 1, 25, record.RecordTypePutOperation)

	err := state.OnPut(putPayload, ctx)
	if err == nil {
		t.Fatal("expected error on put without begin")
	}
	if !errors.Is(err, recovery.ErrOrphanOp) {
		t.Errorf("expected ErrOrphanOp, got %v", err)
	}
}

// TestReplayStateOnDelSuccess tests successful delete operation
func TestReplayStateOnDelSuccess(t *testing.T) {
	mem := memtable.New()
	state := recovery.NewReplayState(mem)

	// Begin transaction
	beginPayload := record.BeginCommitTransactionPayload{TxnID: 1}
	ctx := recovery.NewRecordCtx(0, 1, 8, record.RecordTypeBeginTransaction)
	tst.RequireNoError(t, state.OnBegin(beginPayload, ctx))

	// Add delete operation
	delPayload := record.DeleteOpPayload{
		TxnID: 1,
		Key:   []byte("testkey"),
	}
	ctx = recovery.NewRecordCtx(8, 1, 15, record.RecordTypeDeleteOperation)

	err := state.OnDel(delPayload, ctx)
	tst.RequireNoError(t, err)

	// Verify operation is stored
	inflight := state.Inflight()
	txnBuf, exists := inflight[1]
	tst.AssertTrue(t, exists, "expected transaction 1 to be inflight")
	tst.AssertTrue(t, len(txnBuf.Ops) == 1, "expected 1 operation")
}

// TestReplayStateOnDelWithoutBegin tests delete without active transaction
func TestReplayStateOnDelWithoutBegin(t *testing.T) {
	mem := memtable.New()
	state := recovery.NewReplayState(mem)

	delPayload := record.DeleteOpPayload{
		TxnID: 1,
		Key:   []byte("testkey"),
	}
	ctx := recovery.NewRecordCtx(0, 1, 15, record.RecordTypeDeleteOperation)

	err := state.OnDel(delPayload, ctx)
	if err == nil {
		t.Fatal("expected error on delete without begin")
	}
	if !errors.Is(err, recovery.ErrOrphanOp) {
		t.Errorf("expected ErrOrphanOp, got %v", err)
	}
}

// TestReplayStateOnCommitSuccess tests successful commit
func TestReplayStateOnCommitSuccess(t *testing.T) {
	mem := memtable.New()
	state := recovery.NewReplayState(mem)

	// Begin transaction
	beginPayload := record.BeginCommitTransactionPayload{TxnID: 1}
	ctx := recovery.NewRecordCtx(0, 1, 8, record.RecordTypeBeginTransaction)
	tst.RequireNoError(t, state.OnBegin(beginPayload, ctx))

	// Add operation
	putPayload := record.PutOpPayload{
		TxnID: 1,
		Key:   []byte("testkey"),
		Value: []byte("testvalue"),
	}
	ctx = recovery.NewRecordCtx(8, 1, 25, record.RecordTypePutOperation)
	tst.RequireNoError(t, state.OnPut(putPayload, ctx))

	// Commit
	commitPayload := record.BeginCommitTransactionPayload{TxnID: 1}
	ctx = recovery.NewRecordCtx(40, 1, 8, record.RecordTypeCommitTransaction)

	err := state.OnCommit(commitPayload, ctx)
	tst.RequireNoError(t, err)

	// Verify transaction was removed from inflight
	inflight := state.Inflight()
	_, exists := inflight[1]
	tst.AssertTrue(t, !exists, "expected transaction 1 to be removed after commit")

	// Verify maxCommitted was updated
	tst.AssertTrue(t, state.MaxCommitted() == 1, "expected maxCommitted to be 1")
}

// TestReplayStateOnCommitWithoutBegin tests commit without active transaction
func TestReplayStateOnCommitWithoutBegin(t *testing.T) {
	mem := memtable.New()
	state := recovery.NewReplayState(mem)

	commitPayload := record.BeginCommitTransactionPayload{TxnID: 1}
	ctx := recovery.NewRecordCtx(0, 1, 8, record.RecordTypeCommitTransaction)

	err := state.OnCommit(commitPayload, ctx)
	if err == nil {
		t.Fatal("expected error on commit without begin")
	}
	if !errors.Is(err, recovery.ErrCommitNoTxn) {
		t.Errorf("expected ErrCommitNoTxn, got %v", err)
	}
}

// TestReplayStateOnCommitNonMonotonic tests non-monotonic transaction IDs
func TestReplayStateOnCommitNonMonotonic(t *testing.T) {
	mem := memtable.New()
	state := recovery.NewReplayState(mem)

	// Commit transaction 2
	beginPayload := record.BeginCommitTransactionPayload{TxnID: 2}
	ctx := recovery.NewRecordCtx(0, 1, 8, record.RecordTypeBeginTransaction)
	tst.RequireNoError(t, state.OnBegin(beginPayload, ctx))

	commitPayload := record.BeginCommitTransactionPayload{TxnID: 2}
	ctx = recovery.NewRecordCtx(8, 1, 8, record.RecordTypeCommitTransaction)
	tst.RequireNoError(t, state.OnCommit(commitPayload, ctx))

	// Try to commit transaction 1 (lower than 2)
	beginPayload = record.BeginCommitTransactionPayload{TxnID: 1}
	ctx = recovery.NewRecordCtx(16, 1, 8, record.RecordTypeBeginTransaction)
	tst.RequireNoError(t, state.OnBegin(beginPayload, ctx))

	commitPayload = record.BeginCommitTransactionPayload{TxnID: 1}
	ctx = recovery.NewRecordCtx(24, 1, 8, record.RecordTypeCommitTransaction)

	err := state.OnCommit(commitPayload, ctx)
	if err == nil {
		t.Fatal("expected error on non-monotonic commit")
	}
	if !errors.Is(err, recovery.ErrTxnNotMonotonic) {
		t.Errorf("expected ErrTxnNotMonotonic, got %v", err)
	}
}

// TestReplayStateComplexTransaction tests a full transaction with multiple operations
func TestReplayStateComplexTransaction(t *testing.T) {
	mem := memtable.New()
	state := recovery.NewReplayState(mem)

	// Begin transaction
	beginPayload := record.BeginCommitTransactionPayload{TxnID: 1}
	ctx := recovery.NewRecordCtx(0, 1, 8, record.RecordTypeBeginTransaction)
	tst.RequireNoError(t, state.OnBegin(beginPayload, ctx))

	// Add multiple operations
	putPayload1 := record.PutOpPayload{
		TxnID: 1,
		Key:   []byte("key1"),
		Value: []byte("value1"),
	}
	ctx = recovery.NewRecordCtx(8, 1, 20, record.RecordTypePutOperation)
	tst.RequireNoError(t, state.OnPut(putPayload1, ctx))

	delPayload := record.DeleteOpPayload{
		TxnID: 1,
		Key:   []byte("key2"),
	}
	ctx = recovery.NewRecordCtx(28, 1, 14, record.RecordTypeDeleteOperation)
	tst.RequireNoError(t, state.OnDel(delPayload, ctx))

	putPayload2 := record.PutOpPayload{
		TxnID: 1,
		Key:   []byte("key3"),
		Value: []byte("value3"),
	}
	ctx = recovery.NewRecordCtx(42, 1, 20, record.RecordTypePutOperation)
	tst.RequireNoError(t, state.OnPut(putPayload2, ctx))

	// Verify all operations are buffered
	inflight := state.Inflight()
	txnBuf, exists := inflight[1]
	tst.AssertTrue(t, exists, "expected transaction 1 to be inflight")
	tst.AssertTrue(t, len(txnBuf.Ops) == 3, "expected 3 operations")

	// Commit
	commitPayload := record.BeginCommitTransactionPayload{TxnID: 1}
	ctx = recovery.NewRecordCtx(62, 1, 8, record.RecordTypeCommitTransaction)
	tst.RequireNoError(t, state.OnCommit(commitPayload, ctx))

	// Verify transaction was removed
	inflight = state.Inflight()
	_, exists = inflight[1]
	tst.AssertTrue(t, !exists, "expected transaction 1 to be removed after commit")
	tst.AssertTrue(t, state.MaxCommitted() == 1, "expected maxCommitted to be 1")
}

// TestReplayStateMultipleTransactions tests multiple concurrent transactions
func TestReplayStateMultipleTransactions(t *testing.T) {
	mem := memtable.New()
	state := recovery.NewReplayState(mem)

	// Begin transactions 1 and 2
	for txnID := uint64(1); txnID <= 2; txnID++ {
		beginPayload := record.BeginCommitTransactionPayload{TxnID: txnID}
		ctx := recovery.NewRecordCtx(0, 1, 8, record.RecordTypeBeginTransaction)
		err := state.OnBegin(beginPayload, ctx)
		tst.RequireNoError(t, err)
	}

	// Verify both are inflight
	inflight := state.Inflight()
	tst.AssertTrue(t, len(inflight) == 2, "expected 2 inflight transactions")

	// Commit transaction 1
	commitPayload := record.BeginCommitTransactionPayload{TxnID: 1}
	ctx := recovery.NewRecordCtx(8, 1, 8, record.RecordTypeCommitTransaction)
	tst.RequireNoError(t, state.OnCommit(commitPayload, ctx))

	// Verify only transaction 2 is inflight
	inflight = state.Inflight()
	tst.AssertTrue(t, len(inflight) == 1, "expected 1 inflight transaction")
	_, exists := inflight[2]
	tst.AssertTrue(t, exists, "expected transaction 2 to be inflight")

	// Commit transaction 2
	commitPayload = record.BeginCommitTransactionPayload{TxnID: 2}
	tst.RequireNoError(t, state.OnCommit(commitPayload, ctx))

	// Verify both are committed
	inflight = state.Inflight()
	tst.AssertTrue(t, len(inflight) == 0, "expected 0 inflight transactions")
	tst.AssertTrue(t, state.MaxCommitted() == 2, "expected maxCommitted to be 2")
}
