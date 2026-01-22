package txn

import (
	"errors"
)

var (
	// Returned when next txn ID is 0 or otherwise forbidden.
	ErrInvalidTxnID = errors.New("txn: invalid transaction id")

	// Returned when SetNext attempts to move the allocator backwards.
	ErrTxnIDRegression = errors.New("txn: transaction id regression")

	// Returned when allocator is used before being initialized.
	ErrTxnIDUninitialized = errors.New("txn: transaction id allocator not initialized")

	// Returned when Next would overflow uint64.
	ErrTxnIDOverflow = errors.New("txn: transaction id overflow")

	// Returned when an operation kind is invalid.
	ErrTxnInvalidOpKind = errors.New("txn: invalid operation kind")
)

type TxnIDError struct {
	Err  error
	Have uint64
	Want uint64
}

func (e *TxnIDError) Error() string { return e.Err.Error() }
func (e *TxnIDError) Unwrap() error { return e.Err }
