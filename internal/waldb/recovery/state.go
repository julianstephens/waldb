package recovery

import "github.com/julianstephens/waldb/internal/waldb/txn"

type ReplayStateMachine struct {
	inTxn        bool
	curTxnId     uint64
	pending      []txn.Op
	maxCommitted uint64

	apply func([]txn.Op) error
}

// NewReplayStateMachine creates a new ReplayStateMachine with the given apply function.
// The apply function is called with the list of operations when a transaction is committed.
func NewReplayStateMachine(apply func([]txn.Op) error) *ReplayStateMachine {
	return &ReplayStateMachine{
		apply: apply,
	}
}

// Begin starts a new transaction with the given transaction ID.
// It returns an error if there is already an active transaction.
func (rsm *ReplayStateMachine) Begin(txnId uint64) error {
	if rsm.inTxn {
		return ErrDoubleBegin
	}
	rsm.inTxn = true
	rsm.curTxnId = txnId
	rsm.pending = rsm.pending[:0]
	return nil
}

// AddPut adds a put operation to the current transaction.
// It returns an error if there is no active transaction or if the provided
// transaction ID does not match the current transaction.
func (rsm *ReplayStateMachine) AddPut(txnId uint64, key, value []byte) error {
	if !rsm.inTxn {
		return ErrOrphanOp
	}
	if rsm.curTxnId != txnId {
		return ErrTxnMismatch
	}

	rsm.pending = append(rsm.pending, txn.Op{
		Kind:  txn.OpPut,
		Key:   key,
		Value: value,
	})

	return nil
}

// AddDelete adds a delete operation to the current transaction.
// It returns an error if there is no active transaction or if the provided
// transaction ID does not match the current transaction.
func (rsm *ReplayStateMachine) AddDelete(txnId uint64, key []byte) error {
	if !rsm.inTxn {
		return ErrOrphanOp
	}
	if rsm.curTxnId != txnId {
		return ErrTxnMismatch
	}

	rsm.pending = append(rsm.pending, txn.Op{
		Kind: txn.OpDelete,
		Key:  key,
	})

	return nil
}

// Commit commits the current transaction, applying all pending operations.
// It returns an error if there is no active transaction or if the provided
// transaction ID does not match the current transaction.
func (rsm *ReplayStateMachine) Commit(txnId uint64) error {
	if !rsm.inTxn {
		return ErrCommitNoTxn
	}
	if rsm.curTxnId != txnId {
		return ErrTxnMismatch
	}

	if err := rsm.apply(rsm.pending); err != nil {
		return err
	}

	if txnId > rsm.maxCommitted {
		rsm.maxCommitted = txnId
	}

	rsm.Reset()

	return nil
}

// Reset resets the state machine to its initial state.
// This is used after a commit or to abandon the current transaction.
func (rsm *ReplayStateMachine) Reset() {
	rsm.inTxn = false
	rsm.curTxnId = 0
	rsm.pending = rsm.pending[:0]
}

// MaxCommittedTxnId returns the highest committed transaction ID seen so far.
// If no transactions have been committed, it returns 0.
func (rsm *ReplayStateMachine) MaxCommittedTxnId() uint64 {
	return rsm.maxCommitted
}
