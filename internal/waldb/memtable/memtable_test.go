package memtable_test

import (
	"bytes"
	"sync"
	"testing"

	"github.com/julianstephens/waldb/internal/waldb/kv"
	"github.com/julianstephens/waldb/internal/waldb/memtable"
)

// TestNew creates a new empty memtable
func TestNew(t *testing.T) {
	tbl := memtable.New()
	if tbl == nil {
		t.Fatal("expected non-nil table")
	}

	// Empty table should have no keys
	snap := tbl.Snapshot()
	if len(snap) != 0 {
		t.Errorf("expected empty table, got %d entries", len(snap))
	}
}

// TestPut stores and retrieves a key-value pair
func TestPut(t *testing.T) {
	tbl := memtable.New()
	key := []byte("mykey")
	value := []byte("myvalue")

	err := tbl.Put(key, value)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	retrieved, ok := tbl.Get(key)
	if !ok {
		t.Fatal("expected key to be found")
	}
	if !bytes.Equal(retrieved, value) {
		t.Errorf("expected value %v, got %v", value, retrieved)
	}
}

// TestPutNilKey tests Put with nil key
func TestPutNilKey(t *testing.T) {
	tbl := memtable.New()
	err := tbl.Put(nil, []byte("value"))
	if err == nil {
		t.Fatal("expected error for nil key")
	}
	if err != memtable.ErrNilKey {
		t.Errorf("expected ErrNilKey, got %v", err)
	}
}

// TestPutEmptyKey stores a key-value pair with empty key
func TestPutEmptyKey(t *testing.T) {
	tbl := memtable.New()
	key := []byte{}
	value := []byte("value")

	err := tbl.Put(key, value)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	retrieved, ok := tbl.Get(key)
	if !ok {
		t.Fatal("expected key to be found")
	}
	if !bytes.Equal(retrieved, value) {
		t.Errorf("expected value %v, got %v", value, retrieved)
	}
}

// TestPutEmptyValue stores a key with empty value
func TestPutEmptyValue(t *testing.T) {
	tbl := memtable.New()
	key := []byte("key")
	value := []byte{}

	err := tbl.Put(key, value)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	retrieved, ok := tbl.Get(key)
	if !ok {
		t.Fatal("expected key to be found")
	}
	if len(retrieved) != 0 {
		t.Errorf("expected empty value, got %v", retrieved)
	}
}

// TestPutOverwrite overwrites an existing key
func TestPutOverwrite(t *testing.T) {
	tbl := memtable.New()
	key := []byte("key")
	value1 := []byte("value1")
	value2 := []byte("value2")

	if err := tbl.Put(key, value1); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	retrieved1, _ := tbl.Get(key)
	if !bytes.Equal(retrieved1, value1) {
		t.Errorf("expected value1, got %v", retrieved1)
	}

	if err := tbl.Put(key, value2); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	retrieved2, _ := tbl.Get(key)
	if !bytes.Equal(retrieved2, value2) {
		t.Errorf("expected value2, got %v", retrieved2)
	}
}

// TestGetNilKey tests Get with nil key
func TestGetNilKey(t *testing.T) {
	tbl := memtable.New()
	_, ok := tbl.Get(nil)
	if ok {
		t.Fatal("expected Get(nil) to return false")
	}
}

// TestGetMissing tests Get for non-existent key
func TestGetMissing(t *testing.T) {
	tbl := memtable.New()
	if err := tbl.Put([]byte("key1"), []byte("value1")); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	_, ok := tbl.Get([]byte("key2"))
	if ok {
		t.Fatal("expected Get to return false for missing key")
	}
}

// TestGetDeleted tests that Get returns false for deleted key
func TestGetDeleted(t *testing.T) {
	tbl := memtable.New()
	key := []byte("key")
	if err := tbl.Put(key, []byte("value")); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	err := tbl.Delete(key)
	if err != nil {
		t.Fatalf("unexpected error deleting: %v", err)
	}

	_, ok := tbl.Get(key)
	if ok {
		t.Fatal("expected Get to return false for deleted key")
	}
}

// TestDeleteNilKey tests Delete with nil key
func TestDeleteNilKey(t *testing.T) {
	tbl := memtable.New()
	err := tbl.Delete(nil)
	if err == nil {
		t.Fatal("expected error for nil key")
	}
	if err != memtable.ErrNilKey {
		t.Errorf("expected ErrNilKey, got %v", err)
	}
}

// TestDeleteNonExistent deletes a non-existent key (should be no-op)
func TestDeleteNonExistent(t *testing.T) {
	tbl := memtable.New()
	err := tbl.Delete([]byte("nonexistent"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	_, ok := tbl.Get([]byte("nonexistent"))
	if ok {
		t.Fatal("expected Get to return false")
	}
}

// TestDeleteIdempotent tests that deleting twice is safe
func TestDeleteIdempotent(t *testing.T) {
	tbl := memtable.New()
	key := []byte("key")
	if err := tbl.Put(key, []byte("value")); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	err1 := tbl.Delete(key)
	if err1 != nil {
		t.Fatalf("unexpected error on first delete: %v", err1)
	}

	err2 := tbl.Delete(key)
	if err2 != nil {
		t.Fatalf("unexpected error on second delete: %v", err2)
	}

	_, ok := tbl.Get(key)
	if ok {
		t.Fatal("expected Get to return false after double delete")
	}
}

// TestSnapshot returns a copy of the table state
func TestSnapshot(t *testing.T) {
	tbl := memtable.New()
	if err := tbl.Put([]byte("key1"), []byte("value1")); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := tbl.Put([]byte("key2"), []byte("value2")); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := tbl.Delete([]byte("key3")); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	snap := tbl.Snapshot()
	if len(snap) != 3 {
		t.Errorf("expected 3 entries in snapshot, got %d", len(snap))
	}

	// Verify entries
	if entry, ok := snap["key1"]; !ok || entry.Tombstone || !bytes.Equal(entry.Value, []byte("value1")) {
		t.Error("key1 snapshot entry incorrect")
	}
	if entry, ok := snap["key2"]; !ok || entry.Tombstone || !bytes.Equal(entry.Value, []byte("value2")) {
		t.Error("key2 snapshot entry incorrect")
	}
	if entry, ok := snap["key3"]; !ok || !entry.Tombstone {
		t.Error("key3 should be tombstoned in snapshot")
	}
}

// TestSnapshotIsolation verifies that snapshot changes don't affect table
func TestSnapshotIsolation(t *testing.T) {
	tbl := memtable.New()
	if err := tbl.Put([]byte("key"), []byte("value")); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	snap := tbl.Snapshot()
	snap["key"] = memtable.Entry{Value: []byte("modified"), Tombstone: false}

	// Table should still have original value
	retrieved, ok := tbl.Get([]byte("key"))
	if !ok || !bytes.Equal(retrieved, []byte("value")) {
		t.Error("table value was modified by snapshot change")
	}
}

// TestSnapshotValueCopy verifies snapshot copies values
func TestSnapshotValueCopy(t *testing.T) {
	tbl := memtable.New()
	originalValue := []byte{0x01, 0x02, 0x03}
	if err := tbl.Put([]byte("key"), originalValue); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	snap := tbl.Snapshot()
	snapValue := snap["key"].Value

	// Modify the snapshot value
	if len(snapValue) > 0 {
		snapValue[0] = 0xFF
	}

	// Original table should be unaffected
	retrieved, _ := tbl.Get([]byte("key"))
	if retrieved[0] != 0x01 {
		t.Error("table value was modified by snapshot value modification")
	}
}

// TestApplyPut applies a put operation
func TestApplyPut(t *testing.T) {
	tbl := memtable.New()
	ops := []kv.Op{
		{Kind: kv.OpPut, Key: []byte("key1"), Value: []byte("value1")},
		{Kind: kv.OpPut, Key: []byte("key2"), Value: []byte("value2")},
	}

	err := tbl.Apply(ops)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if val, ok := tbl.Get([]byte("key1")); !ok || !bytes.Equal(val, []byte("value1")) {
		t.Error("key1 not applied correctly")
	}
	if val, ok := tbl.Get([]byte("key2")); !ok || !bytes.Equal(val, []byte("value2")) {
		t.Error("key2 not applied correctly")
	}
}

// TestApplyDelete applies a delete operation
func TestApplyDelete(t *testing.T) {
	tbl := memtable.New()
	if err := tbl.Put([]byte("key"), []byte("value")); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	ops := []kv.Op{
		{Kind: kv.OpDelete, Key: []byte("key")},
	}

	err := tbl.Apply(ops)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	_, ok := tbl.Get([]byte("key"))
	if ok {
		t.Fatal("expected key to be deleted")
	}
}

// TestApplyMixed applies mixed put and delete operations
func TestApplyMixed(t *testing.T) {
	tbl := memtable.New()
	if err := tbl.Put([]byte("existing"), []byte("value")); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	ops := []kv.Op{
		{Kind: kv.OpPut, Key: []byte("new"), Value: []byte("newvalue")},
		{Kind: kv.OpDelete, Key: []byte("existing")},
		{Kind: kv.OpPut, Key: []byte("another"), Value: []byte("another")},
	}

	err := tbl.Apply(ops)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if _, ok := tbl.Get([]byte("new")); !ok {
		t.Error("new key not applied")
	}
	if _, ok := tbl.Get([]byte("existing")); ok {
		t.Error("existing key should be deleted")
	}
	if _, ok := tbl.Get([]byte("another")); !ok {
		t.Error("another key not applied")
	}
}

// TestApplyNilKey tests Apply with nil key in batch
func TestApplyNilKey(t *testing.T) {
	tbl := memtable.New()
	ops := []kv.Op{
		{Kind: kv.OpPut, Key: []byte("key1"), Value: []byte("value1")},
		{Kind: kv.OpPut, Key: nil, Value: []byte("value2")}, // nil key
	}

	err := tbl.Apply(ops)
	if err == nil {
		t.Fatal("expected error for nil key in batch")
	}
	if err != memtable.ErrNilKey {
		t.Errorf("expected ErrNilKey, got %v", err)
	}

	// Note: Apply is not atomic - operations before the error are still applied
	// So key1 should be present even though the batch failed
	if _, ok := tbl.Get([]byte("key1")); !ok {
		t.Error("key1 should have been applied before the error")
	}
}

// TestApplyInvalidOpKind tests Apply with invalid operation kind
func TestApplyInvalidOpKind(t *testing.T) {
	tbl := memtable.New()
	ops := []kv.Op{
		{Kind: kv.OpKind(99), Key: []byte("key"), Value: []byte("value")}, // invalid kind
	}

	err := tbl.Apply(ops)
	if err == nil {
		t.Fatal("expected error for invalid op kind")
	}
}

// TestApplyEmpty applies empty operation list
func TestApplyEmpty(t *testing.T) {
	tbl := memtable.New()
	err := tbl.Apply([]kv.Op{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

// TestApplyLastWriteWins applies multiple writes to same key
func TestApplyLastWriteWins(t *testing.T) {
	tbl := memtable.New()
	ops := []kv.Op{
		{Kind: kv.OpPut, Key: []byte("key"), Value: []byte("value1")},
		{Kind: kv.OpPut, Key: []byte("key"), Value: []byte("value2")},
		{Kind: kv.OpPut, Key: []byte("key"), Value: []byte("value3")},
	}

	err := tbl.Apply(ops)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	retrieved, ok := tbl.Get([]byte("key"))
	if !ok || !bytes.Equal(retrieved, []byte("value3")) {
		t.Errorf("expected value3, got %v", retrieved)
	}
}

// TestApplyPutThenDelete applies put then delete to same key
func TestApplyPutThenDelete(t *testing.T) {
	tbl := memtable.New()
	ops := []kv.Op{
		{Kind: kv.OpPut, Key: []byte("key"), Value: []byte("value")},
		{Kind: kv.OpDelete, Key: []byte("key")},
	}

	err := tbl.Apply(ops)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	_, ok := tbl.Get([]byte("key"))
	if ok {
		t.Fatal("expected key to be deleted")
	}
}

// TestApplyDeleteThenPut applies delete then put to same key
func TestApplyDeleteThenPut(t *testing.T) {
	tbl := memtable.New()
	if err := tbl.Put([]byte("key"), []byte("original")); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	ops := []kv.Op{
		{Kind: kv.OpDelete, Key: []byte("key")},
		{Kind: kv.OpPut, Key: []byte("key"), Value: []byte("new")},
	}

	err := tbl.Apply(ops)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	retrieved, ok := tbl.Get([]byte("key"))
	if !ok || !bytes.Equal(retrieved, []byte("new")) {
		t.Errorf("expected new value, got %v", retrieved)
	}
}

// TestConcurrentReads tests concurrent reads are safe
func TestConcurrentReads(t *testing.T) {
	tbl := memtable.New()
	if err := tbl.Put([]byte("key"), []byte("value")); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var wg sync.WaitGroup
	numGoroutines := 10
	errors := make(chan error, numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			val, ok := tbl.Get([]byte("key"))
			if !ok || !bytes.Equal(val, []byte("value")) {
				errors <- ErrValueMismatch
			}
		}()
	}

	wg.Wait()
	close(errors)
	if len(errors) > 0 {
		t.Error("concurrent reads failed")
	}
}

var ErrValueMismatch = &mockError{msg: "value mismatch"}

type mockError struct {
	msg string
}

func (e *mockError) Error() string {
	return e.msg
}

// TestConcurrentReadWrite tests concurrent reads and writes
func TestConcurrentReadWrite(t *testing.T) {
	tbl := memtable.New()

	var wg sync.WaitGroup
	numWriters := 5
	numReaders := 5

	for i := 0; i < numWriters; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			key := []byte("key")
			value := []byte("value")
			_ = tbl.Put(key, value)
		}(i)
	}

	for i := 0; i < numReaders; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			tbl.Get([]byte("key"))
		}(i)
	}

	wg.Wait()
}

// TestEntryStruct tests Entry struct creation and access
func TestEntryStruct(t *testing.T) {
	value := []byte("test")
	entry := memtable.Entry{Value: value, Tombstone: false}

	if !bytes.Equal(entry.Value, value) {
		t.Errorf("expected value %v, got %v", value, entry.Value)
	}
	if entry.Tombstone {
		t.Error("expected Tombstone to be false")
	}
}

// TestTombstoneEntry tests Entry with tombstone
func TestTombstoneEntry(t *testing.T) {
	entry := memtable.Entry{Value: nil, Tombstone: true}

	if entry.Value != nil {
		t.Errorf("expected nil value, got %v", entry.Value)
	}
	if !entry.Tombstone {
		t.Error("expected Tombstone to be true")
	}
}

// TestOpPutStruct tests Op struct for put operation
func TestOpPutStruct(t *testing.T) {
	op := kv.Op{
		Kind:  kv.OpPut,
		Key:   []byte("key"),
		Value: []byte("value"),
	}

	if op.Kind != kv.OpPut {
		t.Errorf("expected OpPut, got %v", op.Kind)
	}
	if !bytes.Equal(op.Key, []byte("key")) {
		t.Errorf("expected key, got %v", op.Key)
	}
	if !bytes.Equal(op.Value, []byte("value")) {
		t.Errorf("expected value, got %v", op.Value)
	}
}

// TestOpDeleteStruct tests Op struct for delete operation
func TestOpDeleteStruct(t *testing.T) {
	op := kv.Op{
		Kind: kv.OpDelete,
		Key:  []byte("key"),
	}

	if op.Kind != kv.OpDelete {
		t.Errorf("expected OpDelete, got %v", op.Kind)
	}
	if !bytes.Equal(op.Key, []byte("key")) {
		t.Errorf("expected key, got %v", op.Key)
	}
}

// TestLargeValue tests storing large values
func TestLargeValue(t *testing.T) {
	tbl := memtable.New()
	key := []byte("large")
	value := make([]byte, 1000000) // 1MB

	err := tbl.Put(key, value)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	retrieved, ok := tbl.Get(key)
	if !ok || len(retrieved) != len(value) {
		t.Error("large value not stored correctly")
	}
}

// TestManyKeys tests table with many keys
func TestManyKeys(t *testing.T) {
	tbl := memtable.New()
	numKeys := 1000

	// Put many keys
	for i := 0; i < numKeys; i++ {
		key := []byte{byte(i >> 8), byte(i)}
		value := []byte("value")
		_ = tbl.Put(key, value)
	}

	// Verify all can be retrieved
	for i := 0; i < numKeys; i++ {
		key := []byte{byte(i >> 8), byte(i)}
		_, ok := tbl.Get(key)
		if !ok {
			t.Errorf("key %d not found", i)
		}
	}

	snap := tbl.Snapshot()
	if len(snap) != numKeys {
		t.Errorf("expected %d entries in snapshot, got %d", numKeys, len(snap))
	}
}

// TestBinaryData tests with binary (non-UTF8) data
func TestBinaryData(t *testing.T) {
	tbl := memtable.New()
	key := []byte{0x00, 0x01, 0x02, 0xFF}
	value := []byte{0xAA, 0xBB, 0xCC, 0xDD}

	err := tbl.Put(key, value)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	retrieved, ok := tbl.Get(key)
	if !ok || !bytes.Equal(retrieved, value) {
		t.Error("binary data not stored correctly")
	}
}

// TestUnicodeData tests with unicode data
func TestUnicodeData(t *testing.T) {
	tbl := memtable.New()
	key := []byte("🔑")
	value := []byte("🔒")

	err := tbl.Put(key, value)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	retrieved, ok := tbl.Get(key)
	if !ok || !bytes.Equal(retrieved, value) {
		t.Error("unicode data not stored correctly")
	}
}

// TestApplyAtomicity verifies Apply behavior on error
// Note: Apply is NOT atomic - it applies operations sequentially and stops on error
func TestApplyAtomicity(t *testing.T) {
	tbl := memtable.New()

	ops := []kv.Op{
		{Kind: kv.OpPut, Key: []byte("key1"), Value: []byte("value1")},
		{Kind: kv.OpPut, Key: nil, Value: []byte("value2")}, // This will fail
		{Kind: kv.OpPut, Key: []byte("key3"), Value: []byte("value3")},
	}

	err := tbl.Apply(ops)
	if err == nil {
		t.Fatal("expected error due to nil key")
	}

	// key1 was applied before the error occurred
	if _, ok := tbl.Get([]byte("key1")); !ok {
		t.Error("key1 should be present as it was applied before the error")
	}

	// key3 was NOT applied because error occurred before it
	if _, ok := tbl.Get([]byte("key3")); ok {
		t.Error("key3 should not be present as error occurred before it")
	}
}
