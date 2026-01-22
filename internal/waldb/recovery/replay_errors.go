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

type ReplaySourceErrorKind int

const (
	ReplaySourceUnknown ReplaySourceErrorKind = iota
	ReplaySourceSegmentOrder
	ReplaySourceSegmentOpen
	ReplaySourceSegmentClose
	ReplaySourceSegmentMissing
)

var (
	ErrSegmentOrder   = errors.New("recovery: invalid segment order")
	ErrSegmentOpen    = errors.New("recovery: failed to open segment")
	ErrSegmentClose   = errors.New("recovery: failed to close segment")
	ErrSegmentMissing = errors.New("recovery: starting segment missing")
)

type ReplaySourceError struct {
	*errorutil.Coordinates
	Kind  ReplaySourceErrorKind
	Cause error
	Err   error
}

func (e *ReplaySourceError) Error() string {
	coords := ""
	if e.Coordinates != nil {
		coords = e.FormatCoordinates()
	}
	return fmt.Sprintf("recovery: source error %s kind=%d: %v (cause: %v)",
		coords, e.Kind, e.Err, e.Cause,
	)
}

func (e *ReplaySourceError) Unwrap() error {
	switch e.Kind {
	case ReplaySourceSegmentOrder:
		return ErrSegmentOrder
	case ReplaySourceSegmentOpen:
		return ErrSegmentOpen
	case ReplaySourceSegmentClose:
		return ErrSegmentClose
	case ReplaySourceSegmentMissing:
		return ErrSegmentMissing
	default:
		return e.Err
	}
}
