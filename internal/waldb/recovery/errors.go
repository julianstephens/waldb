package recovery

import (
	"errors"
	"fmt"

	"github.com/julianstephens/waldb/internal/waldb/wal/record"
)

type ReplayDecodeError struct {
	RecordOffset int64
	DeclaredLen  uint32
	RecordType   record.RecordType
	Err          error
}

func (e *ReplayDecodeError) Error() string {
	return fmt.Sprintf("recovery: decode error at=%d type=%d declared_len=%d: %v",
		e.RecordOffset, e.RecordType, e.DeclaredLen, e.Err,
	)
}
func (e *ReplayDecodeError) Unwrap() error { return e.Err }

var (
	ErrOrphanOp    = errors.New("recovery: op outside transaction")
	ErrTxnMismatch = errors.New("recovery: txn_id mismatch")
	ErrDoubleBegin = errors.New("recovery: begin while transaction active")
	ErrCommitNoTxn = errors.New("recovery: commit with no active transaction")
)

type ReplayLogicError struct {
	AtOffset int64
	Type     record.RecordType
	TxnID    uint64
	CurTxnID uint64
	Err      error
}

func (e *ReplayLogicError) Error() string { return e.Err.Error() }
func (e *ReplayLogicError) Unwrap() error { return e.Err }
