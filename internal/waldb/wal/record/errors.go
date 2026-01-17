package record

import (
	"errors"
	"fmt"
	"io"
)

var (
	ErrTruncated     = errors.New("record: truncated")
	ErrCorrupt       = errors.New("record: corrupt")
	ErrTooLarge      = errors.New("record: too large")
	ErrInvalidType   = errors.New("record: invalid type")
	ErrInvalidLength = errors.New("record: invalid length (must be > 0)")
)

type ParseErrorKind uint8

const (
	KindTruncated ParseErrorKind = iota
	KindInvalidLength
	KindTooLarge
	KindChecksumMismatch
	KindInvalidType
	KindCorrupt
	KindIO
)

func (k ParseErrorKind) String() string {
	switch k {
	case KindTruncated:
		return "truncated"
	case KindInvalidLength:
		return "invalid_length"
	case KindTooLarge:
		return "too_large"
	case KindInvalidType:
		return "invalid_type"
	case KindChecksumMismatch:
		return "checksum_mismatch"
	case KindCorrupt:
		return "corrupt"
	case KindIO:
		return "io_error"
	default:
		return "unknown"
	}
}

type ParseError struct {
	Kind ParseErrorKind
	// Offset is the starting byte offset of the record (at the length prefix)
	Offset int64
	// SafeTruncateOffset is the byte offset where it is safe to truncate the WAL
	// to remove the invalid tail. For record-level parse failures this should be
	// equal to Offset (start of the failing record).
	SafeTruncateOffset int64
	DeclaredLen        uint32
	// RawType is the raw type byte read from the stream (if available).
	RawType byte
	// RecordType is the parsed/validated type (optional; may be zero value if unknown).
	RecordType RecordType
	Want       int
	Have       int
	Err        error
}

func (e *ParseError) Error() string {
	cause := "<nil>"
	if e.Err != nil {
		cause = e.Err.Error()
	}
	return fmt.Sprintf("record parse error kind=%s offset=%d safe=%d len=%d type=0x%02x want=%d have=%d: %s",
		e.Kind.String(), e.Offset, e.SafeTruncateOffset, e.DeclaredLen, e.RawType, e.Want, e.Have, cause)
}

func (e *ParseError) Unwrap() error {
	return e.Err
}

func (e *ParseError) Is(target error) bool {
	switch target {
	case ErrTruncated:
		return e.Kind == KindTruncated
	case ErrInvalidLength:
		return e.Kind == KindInvalidLength
	case ErrTooLarge:
		return e.Kind == KindTooLarge
	case ErrInvalidType:
		return e.Kind == KindInvalidType
	case ErrCorrupt:
		return e.Kind == KindCorrupt
	}
	return false
}

func AsParseError(err error) (*ParseError, bool) {
	var pe *ParseError
	if errors.As(err, &pe) {
		return pe, true
	}
	return nil, false
}

func IsCleanEOF(err error) bool {
	return errors.Is(err, io.EOF)
}

func IsTruncation(err error) bool {
	return errors.Is(err, ErrTruncated)
}

func IsCorruption(err error) bool {
	return errors.Is(err, ErrCorrupt) || errors.Is(err, ErrInvalidLength) || errors.Is(err, ErrTooLarge) ||
		errors.Is(err, ErrInvalidType)
}

var (
	ErrCodecTruncated = errors.New("record: codec truncated payload")
	ErrCodecCorrupt   = errors.New("record: codec corrupt payload")
	ErrCodecInvalid   = errors.New("record: codec invalid payload")
)

type CodecErrorKind uint8

const (
	CodecTruncated CodecErrorKind = iota
	CodecCorrupt
	CodecInvalid
)

func (k CodecErrorKind) String() string {
	switch k {
	case CodecTruncated:
		return "truncated"
	case CodecCorrupt:
		return "corrupt"
	case CodecInvalid:
		return "invalid"
	default:
		return "unknown"
	}
}

type CodecError struct {
	Kind  CodecErrorKind
	Field string // "txn_id", "key_len", "value_len", etc.
	At    int    // byte offset within payload where failure occurred
	Want  int
	Have  int
	Err   error
}

func (e *CodecError) Error() string {
	return fmt.Sprintf("record: codec %s field=%s at=%d want=%d have=%d: %v",
		e.Kind.String(), e.Field, e.At, e.Want, e.Have, e.Err,
	)
}
func (e *CodecError) Unwrap() error { return e.Err }

func (e *CodecError) Is(target error) bool {
	switch target {
	case ErrCodecTruncated:
		return e.Kind == CodecTruncated
	case ErrCodecCorrupt:
		return e.Kind == CodecCorrupt
	case ErrCodecInvalid:
		return e.Kind == CodecInvalid
	default:
		return false
	}
}
