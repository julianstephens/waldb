package recovery

import (
	"errors"
	"fmt"

	"github.com/julianstephens/waldb/internal/waldb/errorutil"
	"github.com/julianstephens/waldb/internal/waldb/wal/record"
)

type ReplayDecodeError struct {
	*errorutil.Coordinates
	SafeOffset  int64
	DeclaredLen uint32
	RecordType  record.RecordType
	Err         error
}

func (e *ReplayDecodeError) Error() string {
	coords := ""
	if e.Coordinates != nil {
		coords = e.FormatCoordinates()
	}
	return fmt.Sprintf("recovery: decode error %s safe_at=%d type=%d declared_len=%d: %v",
		coords, e.SafeOffset, e.RecordType, e.DeclaredLen, e.Err,
	)
}
func (e *ReplayDecodeError) Unwrap() error { return e.Err }

var (
	ErrSegmentOrder    = errors.New("recovery: invalid segment order")
	ErrSegmentNotFound = errors.New("recovery: starting segment not found")
)

type ReplayLogicErrorKind int

const (
	ReplayLogicUnknown ReplayLogicErrorKind = iota
	ReplayLogicSegmentOrder
	ReplayLogicSegmentOpen
	ReplayLogicSegmentClose
	ReplayLogicBegin
	ReplayLogicPut
	ReplayLogicDel
	ReplayLogicCommit
	ReplayLogicApply
)

type ReplayLogicError struct {
	*errorutil.Coordinates
	Kind       ReplayLogicErrorKind
	RecordType record.RecordType
	CurTxnID   uint64
	Err        error
}

func (e *ReplayLogicError) Error() string { return e.Err.Error() }
func (e *ReplayLogicError) Unwrap() error { return e.Err }
