package recovery_test

import (
	"testing"

	"github.com/julianstephens/waldb/internal/waldb/recovery"
	"github.com/julianstephens/waldb/internal/waldb/txn"
)

func TestNewReplayStateMachine(t *testing.T) {
	applyCount := 0
	apply := func(ops []txn.Op) error {
		applyCount++
		return nil
	}

	rsm := recovery.NewReplayStateMachine(apply)
	if rsm == nil {
		t.Fatal("expected non-nil ReplayStateMachine")
	}

	// Verify initial state
	if rsm.MaxCommittedTxnId() != 0 {
		t.Errorf("expected initial maxCommitted to be 0, got %d", rsm.MaxCommittedTxnId())
	}
}

func TestBeginTransaction(t *testing.T) {
	rsm := recovery.NewReplayStateMachine(func(ops []txn.Op) error {
		return nil
	})

	err := rsm.Begin(1)
	if err != nil {
		t.Errorf("unexpected error on Begin: %v", err)
	}
}

func TestBeginTransactionDoubleBegin(t *testing.T) {
	rsm := recovery.NewReplayStateMachine(func(ops []txn.Op) error {
		return nil
	})

	err := rsm.Begin(1)
	if err != nil {
		t.Fatalf("first Begin failed: %v", err)
	}

	// Second Begin should fail
	err = rsm.Begin(2)
	if err == nil {
		t.Fatal("expected error on double Begin")
	}
	if err != recovery.ErrDoubleBegin {
		t.Errorf("expected ErrDoubleBegin, got %v", err)
	}
}

func TestAddPutWithoutBegin(t *testing.T) {
	rsm := recovery.NewReplayStateMachine(func(ops []txn.Op) error {
		return nil
	})

	err := rsm.AddPut(1, []byte("key"), []byte("value"))
	if err == nil {
		t.Fatal("expected error on AddPut without Begin")
	}
	if err != recovery.ErrOrphanOp {
		t.Errorf("expected ErrOrphanOp, got %v", err)
	}
}

func TestAddDeleteWithoutBegin(t *testing.T) {
	rsm := recovery.NewReplayStateMachine(func(ops []txn.Op) error {
		return nil
	})

	err := rsm.AddDelete(1, []byte("key"))
	if err == nil {
		t.Fatal("expected error on AddDelete without Begin")
	}
	if err != recovery.ErrOrphanOp {
		t.Errorf("expected ErrOrphanOp, got %v", err)
	}
}

func TestAddPutTransactionMismatch(t *testing.T) {
	rsm := recovery.NewReplayStateMachine(func(ops []txn.Op) error {
		return nil
	})

	err := rsm.Begin(1)
	if err != nil {
		t.Fatalf("Begin failed: %v", err)
	}

	// AddPut with different txn ID
	err = rsm.AddPut(2, []byte("key"), []byte("value"))
	if err == nil {
		t.Fatal("expected error on txn ID mismatch")
	}
	if err != recovery.ErrTxnMismatch {
		t.Errorf("expected ErrTxnMismatch, got %v", err)
	}
}

func TestAddDeleteTransactionMismatch(t *testing.T) {
	rsm := recovery.NewReplayStateMachine(func(ops []txn.Op) error {
		return nil
	})

	err := rsm.Begin(1)
	if err != nil {
		t.Fatalf("Begin failed: %v", err)
	}

	// AddDelete with different txn ID
	err = rsm.AddDelete(2, []byte("key"))
	if err == nil {
		t.Fatal("expected error on txn ID mismatch")
	}
	if err != recovery.ErrTxnMismatch {
		t.Errorf("expected ErrTxnMismatch, got %v", err)
	}
}

func TestCommitWithoutBegin(t *testing.T) {
	rsm := recovery.NewReplayStateMachine(func(ops []txn.Op) error {
		return nil
	})

	err := rsm.Commit(1)
	if err == nil {
		t.Fatal("expected error on Commit without Begin")
	}
	if err != recovery.ErrCommitNoTxn {
		t.Errorf("expected ErrCommitNoTxn, got %v", err)
	}
}

func TestCommitTransactionMismatch(t *testing.T) {
	rsm := recovery.NewReplayStateMachine(func(ops []txn.Op) error {
		return nil
	})

	err := rsm.Begin(1)
	if err != nil {
		t.Fatalf("Begin failed: %v", err)
	}

	// Commit with different txn ID
	err = rsm.Commit(2)
	if err == nil {
		t.Fatal("expected error on Commit txn ID mismatch")
	}
	if err != recovery.ErrTxnMismatch {
		t.Errorf("expected ErrTxnMismatch, got %v", err)
	}
}

func TestBasicTransaction(t *testing.T) {
	var appliedOps []txn.Op
	rsm := recovery.NewReplayStateMachine(func(ops []txn.Op) error {
		appliedOps = ops
		return nil
	})

	// Begin transaction
	err := rsm.Begin(1)
	if err != nil {
		t.Fatalf("Begin failed: %v", err)
	}

	// Add put operation
	err = rsm.AddPut(1, []byte("key1"), []byte("value1"))
	if err != nil {
		t.Fatalf("AddPut failed: %v", err)
	}

	// Add delete operation
	err = rsm.AddDelete(1, []byte("key2"))
	if err != nil {
		t.Fatalf("AddDelete failed: %v", err)
	}

	// Commit
	err = rsm.Commit(1)
	if err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// Verify operations were applied
	if len(appliedOps) != 2 {
		t.Errorf("expected 2 operations applied, got %d", len(appliedOps))
	}

	// Verify first operation (put)
	if appliedOps[0].Kind != txn.OpPut {
		t.Errorf("expected first op to be Put, got %v", appliedOps[0].Kind)
	}
	if string(appliedOps[0].Key) != "key1" {
		t.Errorf("expected first op key to be 'key1', got %s", appliedOps[0].Key)
	}
	if string(appliedOps[0].Value) != "value1" {
		t.Errorf("expected first op value to be 'value1', got %s", appliedOps[0].Value)
	}

	// Verify second operation (delete)
	if appliedOps[1].Kind != txn.OpDelete {
		t.Errorf("expected second op to be Delete, got %v", appliedOps[1].Kind)
	}
	if string(appliedOps[1].Key) != "key2" {
		t.Errorf("expected second op key to be 'key2', got %s", appliedOps[1].Key)
	}

	// Verify maxCommitted was updated
	if rsm.MaxCommittedTxnId() != 1 {
		t.Errorf("expected maxCommitted to be 1, got %d", rsm.MaxCommittedTxnId())
	}
}

func TestMultipleTransactions(t *testing.T) {
	var applyCalls [][]txn.Op
	rsm := recovery.NewReplayStateMachine(func(ops []txn.Op) error {
		// Copy ops to preserve them across calls
		opsCopy := make([]txn.Op, len(ops))
		copy(opsCopy, ops)
		applyCalls = append(applyCalls, opsCopy)
		return nil
	})

	// First transaction
	err := rsm.Begin(1)
	if err != nil {
		t.Fatalf("Begin(1) failed: %v", err)
	}

	err = rsm.AddPut(1, []byte("key1"), []byte("value1"))
	if err != nil {
		t.Fatalf("AddPut(1) failed: %v", err)
	}

	err = rsm.Commit(1)
	if err != nil {
		t.Fatalf("Commit(1) failed: %v", err)
	}

	// Second transaction
	err = rsm.Begin(2)
	if err != nil {
		t.Fatalf("Begin(2) failed: %v", err)
	}

	err = rsm.AddPut(2, []byte("key2"), []byte("value2"))
	if err != nil {
		t.Fatalf("AddPut(2) failed: %v", err)
	}

	err = rsm.Commit(2)
	if err != nil {
		t.Fatalf("Commit(2) failed: %v", err)
	}

	// Verify both transactions were applied
	if len(applyCalls) != 2 {
		t.Errorf("expected 2 apply calls, got %d", len(applyCalls))
	}

	// Verify first transaction
	if len(applyCalls[0]) != 1 {
		t.Errorf("expected first transaction to have 1 op, got %d", len(applyCalls[0]))
	}

	// Verify second transaction
	if len(applyCalls[1]) != 1 {
		t.Errorf("expected second transaction to have 1 op, got %d", len(applyCalls[1]))
	}

	// Verify maxCommitted is highest
	if rsm.MaxCommittedTxnId() != 2 {
		t.Errorf("expected maxCommitted to be 2, got %d", rsm.MaxCommittedTxnId())
	}
}

func TestTransactionWithMultipleOperations(t *testing.T) {
	var appliedOps []txn.Op
	rsm := recovery.NewReplayStateMachine(func(ops []txn.Op) error {
		appliedOps = ops
		return nil
	})

	// Begin transaction
	err := rsm.Begin(100)
	if err != nil {
		t.Fatalf("Begin failed: %v", err)
	}

	// Add multiple operations
	operations := []struct {
		kind  txn.OpKind
		key   []byte
		value []byte
	}{
		{txn.OpPut, []byte("a"), []byte("1")},
		{txn.OpPut, []byte("b"), []byte("2")},
		{txn.OpDelete, []byte("c"), nil},
		{txn.OpPut, []byte("d"), []byte("4")},
	}

	for i, op := range operations {
		if op.kind == txn.OpPut {
			err = rsm.AddPut(100, op.key, op.value)
		} else {
			err = rsm.AddDelete(100, op.key)
		}
		if err != nil {
			t.Fatalf("operation %d failed: %v", i, err)
		}
	}

	// Commit
	err = rsm.Commit(100)
	if err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// Verify all operations were applied
	if len(appliedOps) != len(operations) {
		t.Errorf("expected %d operations applied, got %d", len(operations), len(appliedOps))
	}

	for i, op := range appliedOps {
		if op.Kind != operations[i].kind {
			t.Errorf("operation %d: expected kind %v, got %v", i, operations[i].kind, op.Kind)
		}
		if string(op.Key) != string(operations[i].key) {
			t.Errorf("operation %d: expected key %s, got %s", i, operations[i].key, op.Key)
		}
		if operations[i].kind == txn.OpPut {
			if string(op.Value) != string(operations[i].value) {
				t.Errorf("operation %d: expected value %s, got %s", i, operations[i].value, op.Value)
			}
		}
	}
}

func TestReset(t *testing.T) {
	rsm := recovery.NewReplayStateMachine(func(ops []txn.Op) error {
		return nil
	})

	// Begin a transaction
	err := rsm.Begin(1)
	if err != nil {
		t.Fatalf("Begin failed: %v", err)
	}

	// Add an operation
	err = rsm.AddPut(1, []byte("key"), []byte("value"))
	if err != nil {
		t.Fatalf("AddPut failed: %v", err)
	}

	// Reset
	rsm.Reset()

	// Should now be able to begin a new transaction
	err = rsm.Begin(2)
	if err != nil {
		t.Errorf("Begin after Reset failed: %v", err)
	}
}

func TestMaxCommittedTxnIdIncreases(t *testing.T) {
	rsm := recovery.NewReplayStateMachine(func(ops []txn.Op) error {
		return nil
	})

	initialMax := rsm.MaxCommittedTxnId()
	if initialMax != 0 {
		t.Errorf("expected initial max to be 0, got %d", initialMax)
	}

	// Commit transaction with ID 5
	if err := rsm.Begin(5); err != nil {
		t.Fatalf("unexpected error on Begin: %v", err)
	}
	if err := rsm.AddPut(5, []byte("key"), []byte("value")); err != nil {
		t.Fatalf("unexpected error on AddPut: %v", err)
	}
	if err := rsm.Commit(5); err != nil {
		t.Fatalf("unexpected error on Commit: %v", err)
	}

	if rsm.MaxCommittedTxnId() != 5 {
		t.Errorf("expected maxCommitted to be 5, got %d", rsm.MaxCommittedTxnId())
	}

	// Commit transaction with ID 3 (lower than 5)
	if err := rsm.Begin(3); err != nil {
		t.Fatalf("unexpected error on Begin: %v", err)
	}
	if err := rsm.AddPut(3, []byte("key"), []byte("value")); err != nil {
		t.Fatalf("unexpected error on AddPut: %v", err)
	}
	if err := rsm.Commit(3); err != nil {
		t.Fatalf("unexpected error on Commit: %v", err)
	}

	// maxCommitted should still be 5
	if rsm.MaxCommittedTxnId() != 5 {
		t.Errorf("expected maxCommitted to stay 5, got %d", rsm.MaxCommittedTxnId())
	}

	// Commit transaction with ID 10
	if err := rsm.Begin(10); err != nil {
		t.Fatalf("unexpected error on Begin: %v", err)
	}
	if err := rsm.AddPut(10, []byte("key"), []byte("value")); err != nil {
		t.Fatalf("unexpected error on AddPut: %v", err)
	}
	if err := rsm.Commit(10); err != nil {
		t.Fatalf("unexpected error on Commit: %v", err)
	}

	if rsm.MaxCommittedTxnId() != 10 {
		t.Errorf("expected maxCommitted to be 10, got %d", rsm.MaxCommittedTxnId())
	}
}

func TestEmptyTransaction(t *testing.T) {
	var appliedOps []txn.Op
	rsm := recovery.NewReplayStateMachine(func(ops []txn.Op) error {
		appliedOps = ops
		return nil
	})

	// Begin and commit without adding any operations
	err := rsm.Begin(1)
	if err != nil {
		t.Fatalf("Begin failed: %v", err)
	}

	err = rsm.Commit(1)
	if err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// Apply should have been called with empty slice
	if len(appliedOps) != 0 {
		t.Errorf("expected empty operations, got %d", len(appliedOps))
	}

	if rsm.MaxCommittedTxnId() != 1 {
		t.Errorf("expected maxCommitted to be 1, got %d", rsm.MaxCommittedTxnId())
	}
}
