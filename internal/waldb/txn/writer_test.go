package txn_test

import (
	"fmt"
	"testing"

	"github.com/julianstephens/waldb/internal/waldb/txn"
	"github.com/julianstephens/waldb/internal/waldb/wal/record"
)

// MockIDAllocator is a test implementation of IDAllocator
type MockIDAllocator struct {
	nextID uint64
}

func NewMockIDAllocator(startID uint64) *MockIDAllocator {
	return &MockIDAllocator{nextID: startID}
}

func (m *MockIDAllocator) Next() uint64 {
	id := m.nextID
	m.nextID++
	return id
}

func (m *MockIDAllocator) Peek() uint64 {
	return m.nextID
}

func (m *MockIDAllocator) SetNext(next uint64) error {
	if next < 1 {
		return &txn.TxnIDError{
			Err:  txn.ErrInvalidTxnID,
			Have: next,
			Want: 1,
		}
	}
	if next < m.nextID {
		return &txn.TxnIDError{
			Err:  txn.ErrTxnIDRegression,
			Have: next,
			Want: m.nextID,
		}
	}
	m.nextID = next
	return nil
}

// recordedCall represents a call to the log appender
type recordedCall struct {
	method     string // "Append", "Flush", or "FSync"
	recordType record.RecordType
	payload    []byte
}

// fakeLogAppender is a test implementation that records all calls
type fakeLogAppender struct {
	calls             []recordedCall
	failOnAppendIndex int // -1 means no failure
	failOnFlush       bool
	failOnFSync       bool
}

func newFakeLogAppender() *fakeLogAppender {
	return &fakeLogAppender{
		calls:             make([]recordedCall, 0),
		failOnAppendIndex: -1,
	}
}

func (f *fakeLogAppender) Append(typ record.RecordType, payload []byte) (int64, error) {
	appendIndex := len([]*recordedCall{})
	for _, call := range f.calls {
		if call.method == "Append" {
			appendIndex++
		}
	}

	if f.failOnAppendIndex == appendIndex {
		return 0, fmt.Errorf("append failed at index %d", appendIndex)
	}

	// Copy payload to avoid issues with reused buffers
	payloadCopy := make([]byte, len(payload))
	copy(payloadCopy, payload)

	f.calls = append(f.calls, recordedCall{
		method:     "Append",
		recordType: typ,
		payload:    payloadCopy,
	})
	return int64(appendIndex), nil
}

func (f *fakeLogAppender) Flush() error {
	f.calls = append(f.calls, recordedCall{method: "Flush"})
	if f.failOnFlush {
		return fmt.Errorf("flush failed")
	}
	return nil
}

func (f *fakeLogAppender) FSync() error {
	f.calls = append(f.calls, recordedCall{method: "FSync"})
	if f.failOnFSync {
		return fmt.Errorf("fsync failed")
	}
	return nil
}

func (f *fakeLogAppender) Close() error {
	return nil
}

// TestHappyPathWithFSync verifies the call sequence: BEGIN → ops → COMMIT → Flush → FSync
func TestHappyPathWithFSync(t *testing.T) {
	allocator := NewMockIDAllocator(100)
	appender := newFakeLogAppender()
	opts := txn.WriterOpts{FsyncOnCommit: true}

	writer := txn.NewWriter(allocator, appender, opts)

	batch := txn.NewBatch()
	batch.Put([]byte("key1"), []byte("value1"))
	batch.Put([]byte("key2"), []byte("value2"))
	batch.Delete([]byte("key3"))

	txnID, err := writer.Commit(batch)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if txnID != 100 {
		t.Errorf("expected txnID 100, got %d", txnID)
	}

	// Verify call sequence: BEGIN, PUT, PUT, DELETE, COMMIT, Flush, FSync
	expectedSequence := []string{"Append", "Append", "Append", "Append", "Append", "Flush", "FSync"}
	if len(appender.calls) != len(expectedSequence) {
		t.Errorf("expected %d calls, got %d", len(expectedSequence), len(appender.calls))
	}

	for i, expectedMethod := range expectedSequence {
		if appender.calls[i].method != expectedMethod {
			t.Errorf("call %d: expected %s, got %s", i, expectedMethod, appender.calls[i].method)
		}
	}

	// Verify record types
	expectedTypes := []record.RecordType{
		record.RecordTypeBeginTransaction,
		record.RecordTypePutOperation,
		record.RecordTypePutOperation,
		record.RecordTypeDeleteOperation,
		record.RecordTypeCommitTransaction,
	}

	for i, expectedType := range expectedTypes {
		if appender.calls[i].recordType != expectedType {
			t.Errorf("record %d: expected type %v, got %v", i, expectedType, appender.calls[i].recordType)
		}
	}
}

// TestHappyPathWithoutFSync verifies the call sequence: BEGIN → ops → COMMIT → Flush (no FSync)
func TestHappyPathWithoutFSync(t *testing.T) {
	allocator := NewMockIDAllocator(200)
	appender := newFakeLogAppender()
	opts := txn.WriterOpts{FsyncOnCommit: false}

	writer := txn.NewWriter(allocator, appender, opts)

	batch := txn.NewBatch()
	batch.Put([]byte("key1"), []byte("value1"))
	batch.Delete([]byte("key2"))

	txnID, err := writer.Commit(batch)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if txnID != 200 {
		t.Errorf("expected txnID 200, got %d", txnID)
	}

	// Verify call sequence: BEGIN, PUT, DELETE, COMMIT, Flush (no FSync)
	expectedSequence := []string{"Append", "Append", "Append", "Append", "Flush"}
	if len(appender.calls) != len(expectedSequence) {
		t.Errorf("expected %d calls, got %d", len(expectedSequence), len(appender.calls))
	}

	for i, expectedMethod := range expectedSequence {
		if appender.calls[i].method != expectedMethod {
			t.Errorf("call %d: expected %s, got %s", i, expectedMethod, appender.calls[i].method)
		}
	}

	// Verify FSync was not called
	for _, call := range appender.calls {
		if call.method == "FSync" {
			t.Error("FSync should not have been called when FsyncOnCommit=false")
		}
	}
}

// TestFailOnOpAppend verifies: error returned; no COMMIT appended; no Flush/FSync
func TestFailOnOpAppend(t *testing.T) {
	allocator := NewMockIDAllocator(300)
	appender := newFakeLogAppender()
	appender.failOnAppendIndex = 1 // Fail on second append (first operation)
	opts := txn.WriterOpts{FsyncOnCommit: true}

	writer := txn.NewWriter(allocator, appender, opts)

	batch := txn.NewBatch()
	batch.Put([]byte("key1"), []byte("value1"))
	batch.Put([]byte("key2"), []byte("value2"))

	txnID, err := writer.Commit(batch)
	if err == nil {
		t.Fatal("expected error when operation append fails")
	}

	// Should only have BEGIN (first append succeeded)
	if len(appender.calls) != 1 {
		t.Errorf("expected 1 call (BEGIN only), got %d", len(appender.calls))
	}

	if appender.calls[0].recordType != record.RecordTypeBeginTransaction {
		t.Errorf("expected BEGIN, got %v", appender.calls[0].recordType)
	}

	// Verify no Flush or FSync were called
	for _, call := range appender.calls {
		if call.method == "Flush" || call.method == "FSync" {
			t.Errorf("expected no %s call on operation append failure", call.method)
		}
	}

	_ = txnID
}

// TestFailOnCommitAppend verifies: error returned; no Flush/FSync
func TestFailOnCommitAppend(t *testing.T) {
	allocator := NewMockIDAllocator(400)
	appender := newFakeLogAppender()
	appender.failOnAppendIndex = 4 // Fail on fifth append (COMMIT after BEGIN + 3 ops)
	opts := txn.WriterOpts{FsyncOnCommit: true}

	writer := txn.NewWriter(allocator, appender, opts)

	batch := txn.NewBatch()
	batch.Put([]byte("key1"), []byte("value1"))
	batch.Delete([]byte("key2"))
	batch.Put([]byte("key3"), []byte("value3"))

	txnID, err := writer.Commit(batch)
	if err == nil {
		t.Fatal("expected error when COMMIT append fails")
	}

	// Should have BEGIN + 3 operations (PUT, DELETE, PUT) but no COMMIT
	appendCount := 0
	for _, call := range appender.calls {
		if call.method == "Append" {
			appendCount++
		}
	}

	if appendCount != 4 {
		t.Errorf("expected 4 appends (BEGIN + 3 ops), got %d", appendCount)
	}

	// Verify no COMMIT appended
	hasCommit := false
	for _, call := range appender.calls {
		if call.method == "Append" && call.recordType == record.RecordTypeCommitTransaction {
			hasCommit = true
		}
	}
	if hasCommit {
		t.Error("COMMIT should not have been appended on COMMIT append failure")
	}

	// Verify no Flush or FSync were called
	for _, call := range appender.calls {
		if call.method == "Flush" || call.method == "FSync" {
			t.Errorf("expected no %s call on COMMIT append failure", call.method)
		}
	}

	_ = txnID
}

// TestFailOnFlush verifies: error returned; no FSync
func TestFailOnFlush(t *testing.T) {
	allocator := NewMockIDAllocator(500)
	appender := newFakeLogAppender()
	appender.failOnFlush = true
	opts := txn.WriterOpts{FsyncOnCommit: true}

	writer := txn.NewWriter(allocator, appender, opts)

	batch := txn.NewBatch()
	batch.Put([]byte("key"), []byte("value"))

	txnID, err := writer.Commit(batch)
	if err == nil {
		t.Fatal("expected error when Flush fails")
	}

	// Should have appended all records (BEGIN, PUT, COMMIT)
	appendCount := 0
	for _, call := range appender.calls {
		if call.method == "Append" {
			appendCount++
		}
	}

	if appendCount != 3 {
		t.Errorf("expected 3 appends (BEGIN + PUT + COMMIT), got %d", appendCount)
	}

	// Verify Flush was called but FSync was not
	flushCalled := false
	fsyncCalled := false
	for _, call := range appender.calls {
		if call.method == "Flush" {
			flushCalled = true
		}
		if call.method == "FSync" {
			fsyncCalled = true
		}
	}

	if !flushCalled {
		t.Error("Flush should have been called")
	}
	if fsyncCalled {
		t.Error("FSync should not have been called when Flush fails")
	}

	_ = txnID
}

// TestFailOnFSync verifies: error returned
func TestFailOnFSync(t *testing.T) {
	allocator := NewMockIDAllocator(600)
	appender := newFakeLogAppender()
	appender.failOnFSync = true
	opts := txn.WriterOpts{FsyncOnCommit: true}

	writer := txn.NewWriter(allocator, appender, opts)

	batch := txn.NewBatch()
	batch.Put([]byte("key"), []byte("value"))

	txnID, err := writer.Commit(batch)
	if err == nil {
		t.Fatal("expected error when FSync fails")
	}

	// Should have appended all records and called Flush and FSync
	appendCount := 0
	flushCalled := false
	fsyncCalled := false

	for _, call := range appender.calls {
		if call.method == "Append" {
			appendCount++
		}
		if call.method == "Flush" {
			flushCalled = true
		}
		if call.method == "FSync" {
			fsyncCalled = true
		}
	}

	if appendCount != 3 {
		t.Errorf("expected 3 appends, got %d", appendCount)
	}
	if !flushCalled {
		t.Error("Flush should have been called")
	}
	if !fsyncCalled {
		t.Error("FSync should have been called")
	}

	// Verify complete call sequence
	expectedSequence := []string{"Append", "Append", "Append", "Flush", "FSync"}
	if len(appender.calls) != len(expectedSequence) {
		t.Errorf("expected %d calls, got %d", len(expectedSequence), len(appender.calls))
	}

	_ = txnID
}
