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

// MockLogWriter is a test implementation of LogWriter that records all appends
type MockLogWriter struct {
	appends      []MockAppendRecord
	flushCalled  bool
	fsyncCalled  bool
	failOnAppend int  // fail on this append call (-1 means no failure)
	failOnFlush  bool // fail on Flush call
	appendCount  int
}

type MockAppendRecord struct {
	RecordType record.RecordType
	Payload    []byte
}

func NewMockLogWriter() *MockLogWriter {
	return &MockLogWriter{
		appends:      make([]MockAppendRecord, 0),
		failOnAppend: -1,
	}
}

func (m *MockLogWriter) Append(typ record.RecordType, payload []byte) (int64, error) {
	if m.failOnAppend == m.appendCount {
		m.appendCount++
		return 0, fmt.Errorf("append failed at index %d", m.failOnAppend)
	}

	// Copy payload to avoid issues with reused buffers
	payloadCopy := make([]byte, len(payload))
	copy(payloadCopy, payload)

	m.appends = append(m.appends, MockAppendRecord{
		RecordType: typ,
		Payload:    payloadCopy,
	})
	m.appendCount++
	return int64(len(m.appends) - 1), nil
}

func (m *MockLogWriter) Flush() error {
	m.flushCalled = true
	if m.failOnFlush {
		return fmt.Errorf("flush failed")
	}
	return nil
}

func (m *MockLogWriter) FSync() error {
	m.fsyncCalled = true
	return nil
}

func (m *MockLogWriter) Close() error {
	return nil
}

// TestWriterOrderingBeginOpsCommit verifies that BEGIN, ops, COMMIT are appended in order with same txnID
func TestWriterOrderingBeginOpsCommit(t *testing.T) {
	allocator := NewMockIDAllocator(100)
	logWriter := NewMockLogWriter()
	opts := txn.WriterOpts{FsyncOnCommit: false}

	writer := txn.NewWriter(allocator, logWriter, opts)

	// Create a batch with multiple operations
	batch := &txn.Batch{}
	batch.Put([]byte("key1"), []byte("value1"))
	batch.Put([]byte("key2"), []byte("value2"))
	batch.Delete([]byte("key3"))

	txnID, err := writer.Commit(batch)
	if err != nil {
		t.Fatalf("unexpected error during commit: %v", err)
	}

	if txnID != 100 {
		t.Errorf("expected txnID 100, got %d", txnID)
	}

	// Verify order: BEGIN, PUT, PUT, DELETE, COMMIT
	expectedRecordCount := 5 // BEGIN + 2 PUTs + 1 DELETE + COMMIT
	if len(logWriter.appends) != expectedRecordCount {
		t.Errorf("expected %d appended records, got %d", expectedRecordCount, len(logWriter.appends))
	}

	// Verify types are in correct order
	expectedTypes := []record.RecordType{
		record.RecordTypeBeginTransaction,
		record.RecordTypePutOperation,
		record.RecordTypePutOperation,
		record.RecordTypeDeleteOperation,
		record.RecordTypeCommitTransaction,
	}

	for i, expectedType := range expectedTypes {
		if logWriter.appends[i].RecordType != expectedType {
			t.Errorf("record %d: expected type %v, got %v", i, expectedType, logWriter.appends[i].RecordType)
		}
	}

	// Verify all operations have the same txnID
	for i, append := range logWriter.appends {
		if i == 0 {
			// BEGIN record
			decodedBegin, err := record.DecodeBeginTxnPayload(append.Payload)
			if err != nil {
				t.Fatalf("record %d: failed to decode BEGIN: %v", i, err)
			}
			if decodedBegin.TransactionID != 100 {
				t.Errorf("record %d (BEGIN): expected txnID 100, got %d", i, decodedBegin.TransactionID)
			}
		} else if i == len(logWriter.appends)-1 {
			// COMMIT record
			decodedCommit, err := record.DecodeCommitTxnPayload(append.Payload)
			if err != nil {
				t.Fatalf("record %d: failed to decode COMMIT: %v", i, err)
			}
			if decodedCommit.TransactionID != 100 {
				t.Errorf("record %d (COMMIT): expected txnID 100, got %d", i, decodedCommit.TransactionID)
			}
		} else {
			// Operation records (PUT or DELETE)
			switch logWriter.appends[i].RecordType {
			case record.RecordTypePutOperation:
				decodedPut, err := record.DecodePutOpPayload(append.Payload)
				if err != nil {
					t.Fatalf("record %d: failed to decode PUT: %v", i, err)
				}
				if decodedPut.TransactionID != 100 {
					t.Errorf("record %d (PUT): expected txnID 100, got %d", i, decodedPut.TransactionID)
				}
			case record.RecordTypeDeleteOperation:
				decodedDelete, err := record.DecodeDeleteOpPayload(append.Payload)
				if err != nil {
					t.Fatalf("record %d: failed to decode DELETE: %v", i, err)
				}
				if decodedDelete.TransactionID != 100 {
					t.Errorf("record %d (DELETE): expected txnID 100, got %d", i, decodedDelete.TransactionID)
				}
			}
		}
	}
}

// TestWriterEmptyBatch verifies that empty batches only write BEGIN and COMMIT
func TestWriterEmptyBatch(t *testing.T) {
	allocator := NewMockIDAllocator(200)
	logWriter := NewMockLogWriter()
	opts := txn.WriterOpts{FsyncOnCommit: false}

	writer := txn.NewWriter(allocator, logWriter, opts)

	batch := &txn.Batch{}
	txnID, err := writer.Commit(batch)
	if err != nil {
		t.Fatalf("unexpected error during commit: %v", err)
	}

	if txnID != 200 {
		t.Errorf("expected txnID 200, got %d", txnID)
	}

	// Empty batch should still have BEGIN and COMMIT
	if len(logWriter.appends) != 2 {
		t.Errorf("expected 2 records for empty batch, got %d", len(logWriter.appends))
	}

	if logWriter.appends[0].RecordType != record.RecordTypeBeginTransaction {
		t.Errorf("expected first record to be BEGIN, got %v", logWriter.appends[0].RecordType)
	}

	if logWriter.appends[1].RecordType != record.RecordTypeCommitTransaction {
		t.Errorf("expected second record to be COMMIT, got %v", logWriter.appends[1].RecordType)
	}
}

// TestWriterFailOnBeginAppend verifies behavior when BEGIN append fails
func TestWriterFailOnBeginAppend(t *testing.T) {
	allocator := NewMockIDAllocator(300)
	logWriter := NewMockLogWriter()
	logWriter.failOnAppend = 0 // Fail on first append (BEGIN)
	opts := txn.WriterOpts{FsyncOnCommit: false}

	writer := txn.NewWriter(allocator, logWriter, opts)

	batch := &txn.Batch{}
	batch.Put([]byte("key"), []byte("value"))

	txnID, err := writer.Commit(batch)
	if err == nil {
		t.Fatal("expected error when BEGIN append fails")
	}

	// Should have attempted one append (BEGIN) before failing
	if len(logWriter.appends) != 0 {
		t.Errorf("expected 0 successful appends, got %d", len(logWriter.appends))
	}

	// Flush and FSync should NOT have been called
	if logWriter.flushCalled {
		t.Error("Flush should not have been called after BEGIN failure")
	}
	if logWriter.fsyncCalled {
		t.Error("FSync should not have been called after BEGIN failure")
	}

	// Transaction ID should not be valid (but is still returned)
	_ = txnID
}

// TestWriterFailOnOperationAppend verifies behavior when operation append fails
func TestWriterFailOnOperationAppend(t *testing.T) {
	allocator := NewMockIDAllocator(400)
	logWriter := NewMockLogWriter()
	logWriter.failOnAppend = 1 // Fail on second append (first operation)
	opts := txn.WriterOpts{FsyncOnCommit: false}

	writer := txn.NewWriter(allocator, logWriter, opts)

	batch := &txn.Batch{}
	batch.Put([]byte("key1"), []byte("value1"))
	batch.Put([]byte("key2"), []byte("value2"))

	txnID, err := writer.Commit(batch)
	if err == nil {
		t.Fatal("expected error when operation append fails")
	}

	// Should have one successful append (BEGIN) before failure
	if len(logWriter.appends) != 1 {
		t.Errorf("expected 1 successful append, got %d", len(logWriter.appends))
	}

	if logWriter.appends[0].RecordType != record.RecordTypeBeginTransaction {
		t.Errorf("expected first record to be BEGIN, got %v", logWriter.appends[0].RecordType)
	}

	// Flush and FSync should NOT have been called
	if logWriter.flushCalled {
		t.Error("Flush should not have been called after operation failure")
	}
	if logWriter.fsyncCalled {
		t.Error("FSync should not have been called after operation failure")
	}

	_ = txnID
}

// TestWriterFailOnCommitAppend verifies behavior when COMMIT append fails
func TestWriterFailOnCommitAppend(t *testing.T) {
	allocator := NewMockIDAllocator(500)
	logWriter := NewMockLogWriter()
	// Fail on COMMIT: BEGIN (0) + PUT (1) + DELETE (2) + COMMIT (3)
	logWriter.failOnAppend = 3
	opts := txn.WriterOpts{FsyncOnCommit: false}

	writer := txn.NewWriter(allocator, logWriter, opts)

	batch := &txn.Batch{}
	batch.Put([]byte("key1"), []byte("value1"))
	batch.Delete([]byte("key2"))

	txnID, err := writer.Commit(batch)
	if err == nil {
		t.Fatal("expected error when COMMIT append fails")
	}

	// Should have 3 successful appends (BEGIN + PUT + DELETE) before COMMIT fails
	if len(logWriter.appends) != 3 {
		t.Errorf("expected 3 successful appends, got %d", len(logWriter.appends))
	}

	expectedTypes := []record.RecordType{
		record.RecordTypeBeginTransaction,
		record.RecordTypePutOperation,
		record.RecordTypeDeleteOperation,
	}

	for i, expectedType := range expectedTypes {
		if logWriter.appends[i].RecordType != expectedType {
			t.Errorf("record %d: expected type %v, got %v", i, expectedType, logWriter.appends[i].RecordType)
		}
	}

	// Flush and FSync should NOT have been called
	if logWriter.flushCalled {
		t.Error("Flush should not have been called after COMMIT failure")
	}
	if logWriter.fsyncCalled {
		t.Error("FSync should not have been called after COMMIT failure")
	}

	_ = txnID
}

// TestWriterFlushCalledOnSuccess verifies that Flush is called on successful commit
func TestWriterFlushCalledOnSuccess(t *testing.T) {
	allocator := NewMockIDAllocator(600)
	logWriter := NewMockLogWriter()
	opts := txn.WriterOpts{FsyncOnCommit: false}

	writer := txn.NewWriter(allocator, logWriter, opts)

	batch := &txn.Batch{}
	batch.Put([]byte("key"), []byte("value"))

	_, err := writer.Commit(batch)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Flush should have been called
	if !logWriter.flushCalled {
		t.Error("Flush should have been called on successful commit")
	}

	// FSync should NOT have been called (FsyncOnCommit is false)
	if logWriter.fsyncCalled {
		t.Error("FSync should not have been called when FsyncOnCommit is false")
	}
}

// TestWriterFSyncCalledOnOption verifies that FSync is called when FsyncOnCommit is true
func TestWriterFSyncCalledOnOption(t *testing.T) {
	allocator := NewMockIDAllocator(700)
	logWriter := NewMockLogWriter()
	opts := txn.WriterOpts{FsyncOnCommit: true}

	writer := txn.NewWriter(allocator, logWriter, opts)

	batch := &txn.Batch{}
	batch.Put([]byte("key"), []byte("value"))

	_, err := writer.Commit(batch)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Both Flush and FSync should have been called
	if !logWriter.flushCalled {
		t.Error("Flush should have been called on successful commit")
	}

	if !logWriter.fsyncCalled {
		t.Error("FSync should have been called when FsyncOnCommit is true")
	}
}

// TestWriterMultipleBatches verifies that multiple commits work correctly
func TestWriterMultipleBatches(t *testing.T) {
	allocator := NewMockIDAllocator(800)
	logWriter := NewMockLogWriter()
	opts := txn.WriterOpts{FsyncOnCommit: false}

	writer := txn.NewWriter(allocator, logWriter, opts)

	// First batch
	batch1 := &txn.Batch{}
	batch1.Put([]byte("key1"), []byte("value1"))

	txnID1, err := writer.Commit(batch1)
	if err != nil {
		t.Fatalf("first commit failed: %v", err)
	}
	if txnID1 != 800 {
		t.Errorf("expected first txnID 800, got %d", txnID1)
	}

	// Second batch
	batch2 := &txn.Batch{}
	batch2.Put([]byte("key2"), []byte("value2"))
	batch2.Delete([]byte("key1"))

	txnID2, err := writer.Commit(batch2)
	if err != nil {
		t.Fatalf("second commit failed: %v", err)
	}
	if txnID2 != 801 {
		t.Errorf("expected second txnID 801, got %d", txnID2)
	}

	// First batch: BEGIN (0) + PUT (1) + COMMIT (2) = 3 records
	// Second batch: BEGIN (3) + PUT (4) + DELETE (5) + COMMIT (6) = 4 records
	// Total: 7 records
	if len(logWriter.appends) != 7 {
		t.Errorf("expected 7 total records, got %d", len(logWriter.appends))
	}

	// Verify txnIDs are different
	decodedBegin1, _ := record.DecodeBeginTxnPayload(logWriter.appends[0].Payload)
	decodedBegin2, _ := record.DecodeBeginTxnPayload(logWriter.appends[3].Payload)

	if decodedBegin1.TransactionID == decodedBegin2.TransactionID {
		t.Error("different transactions should have different txnIDs")
	}
}

// TestWriterPayloadsAreCorrect verifies that payloads contain correct data
func TestWriterPayloadsAreCorrect(t *testing.T) {
	allocator := NewMockIDAllocator(900)
	logWriter := NewMockLogWriter()
	opts := txn.WriterOpts{FsyncOnCommit: false}

	writer := txn.NewWriter(allocator, logWriter, opts)

	batch := &txn.Batch{}
	batch.Put([]byte("mykey"), []byte("myvalue"))
	batch.Delete([]byte("delkey"))

	txnID, err := writer.Commit(batch)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify PUT operation payload
	decodedPut, err := record.DecodePutOpPayload(logWriter.appends[1].Payload)
	if err != nil {
		t.Fatalf("failed to decode PUT: %v", err)
	}
	if decodedPut.TransactionID != txnID {
		t.Errorf("PUT txnID mismatch: expected %d, got %d", txnID, decodedPut.TransactionID)
	}
	if string(decodedPut.Key) != "mykey" {
		t.Errorf("PUT key mismatch: expected 'mykey', got '%s'", decodedPut.Key)
	}
	if string(decodedPut.Value) != "myvalue" {
		t.Errorf("PUT value mismatch: expected 'myvalue', got '%s'", decodedPut.Value)
	}

	// Verify DELETE operation payload
	decodedDelete, err := record.DecodeDeleteOpPayload(logWriter.appends[2].Payload)
	if err != nil {
		t.Fatalf("failed to decode DELETE: %v", err)
	}
	if decodedDelete.TransactionID != txnID {
		t.Errorf("DELETE txnID mismatch: expected %d, got %d", txnID, decodedDelete.TransactionID)
	}
	if string(decodedDelete.Key) != "delkey" {
		t.Errorf("DELETE key mismatch: expected 'delkey', got '%s'", decodedDelete.Key)
	}
}

// TestWriterFlushFailureStopsExecution verifies that if Flush fails, Commit returns error and FSync is not called
func TestWriterFlushFailureStopsExecution(t *testing.T) {
	allocator := NewMockIDAllocator(1000)
	logWriter := NewMockLogWriter()
	logWriter.failOnFlush = true
	opts := txn.WriterOpts{FsyncOnCommit: false}

	writer := txn.NewWriter(allocator, logWriter, opts)

	batch := &txn.Batch{}
	batch.Put([]byte("key"), []byte("value"))

	_, err := writer.Commit(batch)
	if err == nil {
		t.Fatal("expected error when Flush fails")
	}

	// Flush should have been called
	if !logWriter.flushCalled {
		t.Error("Flush should have been called")
	}

	// FSync should NOT have been called
	if logWriter.fsyncCalled {
		t.Error("FSync should not have been called after Flush failure")
	}
}

// TestWriterFlushFailureWithFSyncOption verifies that FSync is not called if Flush fails even with FsyncOnCommit=true
func TestWriterFlushFailureWithFSyncOption(t *testing.T) {
	allocator := NewMockIDAllocator(1100)
	logWriter := NewMockLogWriter()
	logWriter.failOnFlush = true
	opts := txn.WriterOpts{FsyncOnCommit: true} // FsyncOnCommit is true

	writer := txn.NewWriter(allocator, logWriter, opts)

	batch := &txn.Batch{}
	batch.Put([]byte("key"), []byte("value"))

	_, err := writer.Commit(batch)
	if err == nil {
		t.Fatal("expected error when Flush fails")
	}

	// Flush should have been called
	if !logWriter.flushCalled {
		t.Error("Flush should have been called")
	}

	// FSync should NOT have been called because Flush failed first
	if logWriter.fsyncCalled {
		t.Error("FSync should not have been called when Flush fails, even with FsyncOnCommit=true")
	}
}
