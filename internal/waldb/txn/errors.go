package txn

import (
	"errors"
	"fmt"
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

var (
	ErrInvalidBatch  = errors.New("txn: invalid batch")
	ErrEmptyBatch    = errors.New("txn: empty batch")
	ErrInvalidOp     = errors.New("txn: invalid operation")
	ErrEmptyKey      = errors.New("txn: empty key")
	ErrKeyTooLarge   = errors.New("txn: key too large")
	ErrValueTooLarge = errors.New("txn: value too large")
)

type BatchValidationError struct {
	Err error

	OpIndex int // index in batch.ops, or -1 for batch-level errors

	OpKind OpKind // Put/Delete, if applicable

	// Size context
	KeyLen   int
	ValueLen int

	Cause error
}

func (e *BatchValidationError) Error() string {
	if e.OpIndex >= 0 {
		return fmt.Sprintf(
			"txn batch validation failed at op %d: %v",
			e.OpIndex, e.Err,
		)
	}
	return fmt.Sprintf("txn batch validation failed: %v", e.Err)
}

func (e *BatchValidationError) Unwrap() error {
	return e.Err
}

var (
	ErrCommitFailed       = errors.New("txn: commit failed")
	ErrCommitInvalidBatch = errors.New("txn: commit invalid batch")

	ErrCommitEncodeBegin  = errors.New("txn: commit encode BEGIN failed")
	ErrCommitEncodeOp     = errors.New("txn: commit encode op failed")
	ErrCommitEncodeCommit = errors.New("txn: commit encode COMMIT failed")

	ErrCommitAppendBegin  = errors.New("txn: commit append BEGIN failed")
	ErrCommitAppendOp     = errors.New("txn: commit append op failed")
	ErrCommitAppendCommit = errors.New("txn: commit append COMMIT failed")

	ErrCommitFlush = errors.New("txn: commit flush failed")
	ErrCommitFSync = errors.New("txn: commit fsync failed")
)

// CommitStage is where the commit failed. Useful for tests and debugging.
type CommitStage uint8

const (
	StageUnknown CommitStage = iota
	StageValidateBatch
	StageAllocTxnID

	StageEncodeBegin
	StageEncodeOp
	StageEncodeCommit

	StageAppendBegin
	StageAppendOp
	StageAppendCommit

	StageFlush
	StageFSync
)

func (s CommitStage) String() string {
	switch s {
	case StageValidateBatch:
		return "validate_batch"
	case StageAllocTxnID:
		return "alloc_txn_id"
	case StageEncodeBegin:
		return "encode_begin"
	case StageEncodeOp:
		return "encode_op"
	case StageEncodeCommit:
		return "encode_commit"
	case StageAppendBegin:
		return "append_begin"
	case StageAppendOp:
		return "append_op"
	case StageAppendCommit:
		return "append_commit"
	case StageFlush:
		return "flush"
	case StageFSync:
		return "fsync"
	default:
		return "unknown"
	}
}

// CommitError wraps commit failures with stable sentinel + rich context.
type CommitError struct {
	Err   error
	Stage CommitStage

	// Context
	TxnID uint64

	OpIndex int // for StageEncodeOp/StageAppendOp, else -1
	OpKind  OpKind

	// Optional WAL context when an append succeeds partially.
	SegID  uint64
	Offset int64

	// For sizing / diagnostics (optional; set when relevant)
	KeyLen   int
	ValueLen int

	// Underlying cause (codec error, io error, etc.)
	Cause error
}

func (e *CommitError) Error() string {
	base := fmt.Sprintf("txn commit failed (%s)", e.Stage.String())
	if e.TxnID != 0 {
		base = fmt.Sprintf("%s txn_id=%d", base, e.TxnID)
	}
	if e.OpIndex >= 0 {
		base = fmt.Sprintf("%s op=%d", base, e.OpIndex)
	}
	return base
}

func (e *CommitError) Unwrap() error   { return e.Err }
func (e *CommitError) CauseErr() error { return e.Cause }

func wrapCommitErr(stage CommitStage, sentinel error, txnID uint64, cause error) error {
	return &CommitError{
		Err:     sentinel,
		Stage:   stage,
		TxnID:   txnID,
		OpIndex: -1,
		Cause:   cause,
	}
}

func wrapCommitOpErr(stage CommitStage, sentinel error, txnID uint64, opIndex int, op Op, cause error) error {
	ce := &CommitError{
		Err:     sentinel,
		Stage:   stage,
		TxnID:   txnID,
		OpIndex: opIndex,
		OpKind:  op.Kind,
		Cause:   cause,
	}
	if op.Key != nil {
		ce.KeyLen = len(op.Key)
	}
	if op.Kind == OpPut && op.Value != nil {
		ce.ValueLen = len(op.Value)
	}
	return ce
}
