package txn

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
	return b.ops
}
