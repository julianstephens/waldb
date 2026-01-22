package recovery

import (
	"errors"
	"fmt"
)

var (
	ErrOrphanOp        = errors.New("recovery: op outside transaction")
	ErrDoubleBegin     = errors.New("recovery: begin while transaction active")
	ErrTxnInvalidID    = errors.New("recovery: invalid txn_id")
	ErrTxnMismatch     = errors.New("recovery: txn_id mismatch")
	ErrTxnNotMonotonic = errors.New("recovery: non-monotonic txn_id")
	ErrAfterCommit     = errors.New("recovery: record after commit")
	ErrDoubleCommit    = errors.New("recovery: double commit")
)

type StateErrorKind uint8

const (
	StateUnknown StateErrorKind = iota
	StateOrphanOp
	StateDoubleBegin
	StateTxnInvalidID
	StateTxnMismatch
	StateTxnNotMonotonic
	StateAfterCommit
	StateDoubleCommit
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
	case StateOrphanOp:
		return fmt.Sprintf("op %q outside transaction (txn_id=%d)", e.Op, e.TxnID)
	case StateDoubleBegin:
		return fmt.Sprintf("begin while transaction active (txn_id=%d)", e.TxnID)
	case StateTxnMismatch:
		return fmt.Sprintf("txn_id mismatch on %q: have %d, want %d", e.Op, e.HaveTxnID, e.WantTxnID)
	case StateTxnNotMonotonic:
		return fmt.Sprintf("non-monotonic txn_id on %q: have %d, want >= %d", e.Op, e.HaveTxnID, e.WantTxnID)
	case StateTxnInvalidID:
		return fmt.Sprintf("invalid txn_id on %q: %d", e.Op, e.TxnID)
	case StateAfterCommit:
		return fmt.Sprintf("record %q after commit (txn_id=%d)", e.Op, e.TxnID)
	case StateDoubleCommit:
		return fmt.Sprintf("double commit (txn_id=%d)", e.TxnID)
	default:
		return "unknown state error"
	}
}

func (e *StateError) Unwrap() error {
	switch e.Kind {
	case StateOrphanOp:
		return ErrOrphanOp
	case StateDoubleBegin:
		return ErrDoubleBegin
	case StateTxnInvalidID:
		return ErrTxnInvalidID
	case StateTxnMismatch:
		return ErrTxnMismatch
	case StateTxnNotMonotonic:
		return ErrTxnNotMonotonic
	case StateAfterCommit:
		return ErrAfterCommit
	case StateDoubleCommit:
		return ErrDoubleCommit
	default:
		return nil
	}
}
