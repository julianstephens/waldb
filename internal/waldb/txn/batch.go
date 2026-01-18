package txn

import "github.com/julianstephens/waldb/internal/waldb/wal/record"

// Batch represents a collection of operations
// that can be applied as a single transaction.
type Batch struct {
	ops []Op
}

// NewBatch creates and returns a new empty Batch.
func NewBatch() *Batch {
	return &Batch{
		ops: make([]Op, 0),
	}
}

// Put adds a put operation to the batch.
// The key and value are provided as byte slices.
func (b *Batch) Put(key, value []byte) {
	b.ops = append(b.ops, Op{
		Kind:  OpPut,
		Key:   key,
		Value: value,
	})
}

// Delete adds a delete operation to the batch.
// The key to be deleted is provided as a byte slice.
func (b *Batch) Delete(key []byte) {
	b.ops = append(b.ops, Op{
		Kind: OpDelete,
		Key:  key,
	})
}

// Ops returns the list of operations accumulated in the batch.
func (b *Batch) Ops() []Op {
	opsCopy := make([]Op, len(b.ops))
	copy(opsCopy, b.ops)
	return opsCopy
}

// Validate checks the batch for common errors.
// It returns a BatchValidationError if any issues are found.
func (b *Batch) Validate() error {
	if len(b.ops) == 0 {
		return &BatchValidationError{
			Err:     ErrEmptyBatch,
			OpIndex: -1,
		}
	}

	for i, op := range b.ops {
		if (op.Kind != OpPut) && (op.Kind != OpDelete) {
			return &BatchValidationError{
				Err:     ErrInvalidOp,
				OpIndex: i,
				OpKind:  op.Kind,
			}
		}
		if len(op.Key) == 0 {
			return &BatchValidationError{
				Err:     ErrEmptyKey,
				OpIndex: i,
				OpKind:  op.Kind,
			}
		}
		if len(op.Key) > record.MaxKeySize {
			return &BatchValidationError{
				Err:     ErrKeyTooLarge,
				OpIndex: i,
				OpKind:  op.Kind,
				KeyLen:  len(op.Key),
			}
		}
		if (op.Kind == OpPut) && (len(op.Value) > record.MaxValueSize) {
			return &BatchValidationError{
				Err:      ErrValueTooLarge,
				OpIndex:  i,
				OpKind:   op.Kind,
				KeyLen:   len(op.Key),
				ValueLen: len(op.Value),
			}
		}
	}

	return nil
}
