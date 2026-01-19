package recovery

import (
	"errors"
	"fmt"
)

var (
	ErrOrphanOp        = errors.New("recovery: op outside transaction")
	ErrCommitNoTxn     = errors.New("recovery: commit with no active transaction")
	ErrDoubleBegin     = errors.New("recovery: begin while transaction active")
	ErrTxnMismatch     = errors.New("recovery: txn_id mismatch")
	ErrTxnNotMonotonic = errors.New("recovery: non-monotonic txn_id")
	ErrAfterCommit     = errors.New("recovery: record after commit")
)

type StateErrorKind uint8

const (
	StateUnknown StateErrorKind = iota
	StateOrphanOp
	StateCommitNoTxn
	StateDoubleBegin
	StateTxnMismatch
	StateTxnNotMonotonic
	StateAfterCommit
)

type StateError struct {
	Kind StateErrorKind

	// Semantic context (no seg/offset here).
	TxnID uint64 // txn id on the record (if present)

	// When we expected a particular txn id (e.g. mismatch, monotonic rules).
	WantTxnID uint64
	HaveTxnID uint64

	// Optional: what operation triggered it (begin/put/del/commit).
	Op string // "BEGIN", "PUT", "DEL", "COMMIT" (or a small enum)

	// Underlying cause if this error is wrapping another semantic error.
	Cause error
}

func (e *StateError) Error() string {
	// Keep it compact and stable; replay will add coordinates.
	switch e.Kind {
	case StateTxnMismatch:
		return fmt.Sprintf("recovery state error: txn mismatch have=%d want=%d", e.HaveTxnID, e.WantTxnID)
	case StateTxnNotMonotonic:
		return fmt.Sprintf("recovery state error: txn not monotonic have=%d want>=%d", e.HaveTxnID, e.WantTxnID)
	default:
		return "recovery state error"
	}
}

func (e *StateError) Unwrap() error {
	// Return the sentinel corresponding to Kind.
	switch e.Kind {
	case StateOrphanOp:
		return ErrOrphanOp
	case StateCommitNoTxn:
		return ErrCommitNoTxn
	case StateDoubleBegin:
		return ErrDoubleBegin
	case StateTxnMismatch:
		return ErrTxnMismatch
	case StateTxnNotMonotonic:
		return ErrTxnNotMonotonic
	case StateAfterCommit:
		return ErrAfterCommit
	default:
		return nil
	}
}
