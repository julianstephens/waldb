package txn

import (
	"errors"
	"fmt"

	"github.com/julianstephens/waldb/internal/waldb/kv"
)

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

	OpKind kv.OpKind // Put/Delete, if applicable

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
