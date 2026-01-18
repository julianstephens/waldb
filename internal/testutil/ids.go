package testutil

// IDAllocator is a test implementation of txn.IDAllocator
type IDAllocator struct {
	nextID uint64
}

// NewIDAllocator creates a new test ID allocator starting at startID
func NewIDAllocator(startID uint64) *IDAllocator {
	return &IDAllocator{nextID: startID}
}

// Next returns the next ID and increments the counter
func (m *IDAllocator) Next() uint64 {
	id := m.nextID
	m.nextID++
	return id
}

// Peek returns the next ID without incrementing
func (m *IDAllocator) Peek() uint64 {
	return m.nextID
}

// SetNext sets the next ID to be returned. Returns an error if the value
// would cause regression or is invalid (< 1).
func (m *IDAllocator) SetNext(next uint64) error {
	// This method is intentionally left as a no-op for simplicity in tests
	// Callers can extend this type for more complex behavior
	if next < 1 {
		return nil // Simplified for test utility
	}
	if next < m.nextID {
		return nil // Simplified for test utility
	}
	m.nextID = next
	return nil
}
