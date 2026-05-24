package memtable

import (
	"bytes"
	"errors"
	"sync"

	"github.com/julianstephens/waldb/internal/waldb/kv"
)

var (
	ErrNilKey        = errors.New("memtable: nil key")
	ErrInvalidKey    = errors.New("memtable: invalid key")
	ErrInvalidOpKind = errors.New("memtable: invalid op kind")
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
	return bytes.Clone(e.Value), true
}

// Put sets key to value.
func (t *Table) Put(key, value []byte) error {
	if key == nil {
		return ErrNilKey
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	k := string(key)
	v := bytes.Clone(value)

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

	if err := validateOps(ops); err != nil {
		return err
	}

	for _, op := range ops {
		switch op.Kind {
		case kv.OpPut:
			t.putLocked(op.Key, op.Value)
		case kv.OpDelete:
			t.deleteLocked(op.Key)
		}
	}
	return nil
}

// putLocked sets key to value without validation.
// Caller must hold write lock.
func (t *Table) putLocked(key, value []byte) {
	t.m[string(key)] = Entry{Value: bytes.Clone(value), Tombstone: false}
}

// deleteLocked marks key as deleted (tombstone) without validation.
// Caller must hold write lock.
func (t *Table) deleteLocked(key []byte) {
	t.m[string(key)] = Entry{Tombstone: true}
}

// validateOps checks that all operations are valid (non-nil/empty keys, valid kinds).
func validateOps(ops []kv.Op) error {
	for _, op := range ops {
		switch op.Kind {
		case kv.OpPut, kv.OpDelete:
			if op.Key == nil {
				return ErrNilKey
			}
			if len(op.Key) == 0 {
				return ErrInvalidKey
			}
		default:
			return ErrInvalidOpKind
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
		v := bytes.Clone(e.Value)
		out[k] = Entry{Value: v, Tombstone: e.Tombstone}
	}
	return out
}
