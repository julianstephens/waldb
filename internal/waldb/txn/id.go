package txn

import "sync"

// IDAllocator is responsible for handing out monotonically increasing txn IDs
// during a single process lifetime.
type IDAllocator interface {
	// Next reserves and returns the next transaction ID.
	// 0 is reserved as "unset".
	Next() uint64

	// Peek returns the next ID that would be handed out without reserving it.
	Peek() uint64

	// SetNext sets the next ID to be allocated.
	// Used during Open() after recovery computes maxTxnIDSeen.
	SetNext(next uint64) error
}

// CounterAllocator is the default in-memory implementation.
type CounterAllocator struct {
	mu   sync.Mutex
	next uint64
}

// NewCounterAllocator constructs an allocator starting at `next`.
// For a fresh DB, pass next=1. After recovery, pass maxTxnIDSeen+1.
func NewCounterAllocator(next uint64) (*CounterAllocator, error) {
	if next < 1 {
		return nil, &TxnIDError{
			Err:  ErrInvalidTxnID,
			Have: next,
			Want: 1,
		}
	}
	return &CounterAllocator{next: next}, nil
}

// Next reserves and returns the next transaction ID.
func (a *CounterAllocator) Next() uint64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.next++
	return a.next - 1
}

// Peek returns the next ID without reserving it.
func (a *CounterAllocator) Peek() uint64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.next
}

// SetNext sets the next ID to be allocated.
// Intended for initialization at Open(), and (optionally) for repair workflows.
func (a *CounterAllocator) SetNext(next uint64) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	if next < 1 {
		return &TxnIDError{
			Err:  ErrInvalidTxnID,
			Have: next,
			Want: 1,
		}
	}
	if next < a.next {
		return &TxnIDError{
			Err:  ErrTxnIDRegression,
			Have: next,
			Want: a.next,
		}
	}

	a.next = next
	return nil
}
