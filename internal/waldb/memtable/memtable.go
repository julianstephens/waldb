package memtable

import (
	"errors"
	"sync"

	"github.com/julianstephens/waldb/internal/waldb/kv"
)

var (
	ErrNilKey = errors.New("memtable: nil key")
)

// Entry represents a stored value or a tombstone.
type Entry struct {
	Value     []byte
	Tombstone bool
}

// Table is an in-memory key/value table with tombstones.
type Table struct {
	mu sync.RWMutex
	m  map[string]Entry
}

// New creates an empty memtable.
func New() *Table {
	return &Table{
		m: make(map[string]Entry),
	}
}

// Get returns the value for key if present and not tombstoned.
func (t *Table) Get(key []byte) (value []byte, ok bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	if key == nil {
		return nil, false
	}

	e, ok := t.m[string(key)]
	if !ok || e.Tombstone {
		return nil, false
	}
	return e.Value, true
}

// Put sets key to value.
func (t *Table) Put(key, value []byte) error {
	if key == nil {
		return ErrNilKey
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	k := string(key)
	v := make([]byte, len(value))
	copy(v, value)

	t.m[k] = Entry{Value: v, Tombstone: false}
	return nil
}

// Delete marks key as deleted (tombstone).
func (t *Table) Delete(key []byte) error {
	if key == nil {
		return ErrNilKey
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	t.m[string(key)] = Entry{Value: nil, Tombstone: true}
	return nil
}

// Apply atomically applies a batch of operations. Either all ops are applied or none.
func (t *Table) Apply(ops []kv.Op) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	for _, op := range ops {
		if op.Key == nil {
			return ErrNilKey
		}
		switch op.Kind {
		case kv.OpPut:
			v := make([]byte, len(op.Value))
			copy(v, op.Value)
			t.m[string(op.Key)] = Entry{Value: v, Tombstone: false}
		case kv.OpDelete:
			t.m[string(op.Key)] = Entry{Value: nil, Tombstone: true}
		default:
			return errors.New("memtable: invalid op kind")
		}
	}
	return nil
}

// Snapshot returns a copy of the current state (for tests/debugging).
func (t *Table) Snapshot() map[string]Entry {
	t.mu.RLock()
	defer t.mu.RUnlock()

	out := make(map[string]Entry, len(t.m))
	for k, e := range t.m {
		// Copy value to avoid sharing memory.
		v := make([]byte, len(e.Value))
		copy(v, e.Value)
		out[k] = Entry{Value: v, Tombstone: e.Tombstone}
	}
	return out
}
