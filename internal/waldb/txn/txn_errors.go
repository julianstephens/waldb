package txn

import (
	"errors"
	"fmt"

	"github.com/julianstephens/waldb/internal/waldb/kv"
)

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
	OpKind  kv.OpKind

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

func wrapCommitOpErr(stage CommitStage, sentinel error, txnID uint64, opIndex int, op kv.Op, cause error) error {
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
	if op.Kind == kv.OpPut && op.Value != nil {
		ce.ValueLen = len(op.Value)
	}
	return ce
}
