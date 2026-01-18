package txn_test

import (
	"testing"

	"github.com/julianstephens/waldb/internal/testutil"
	"github.com/julianstephens/waldb/internal/waldb/txn"
	"github.com/julianstephens/waldb/internal/waldb/wal/record"
)

// TestHappyPathWithFSync verifies the call sequence: BEGIN → ops → COMMIT → Flush → FSync
func TestHappyPathWithFSync(t *testing.T) {
	allocator := testutil.NewIDAllocator(100)
	appender := testutil.NewLogAppender()
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
	if len(appender.Calls()) != len(expectedSequence) {
		t.Errorf("expected %d calls, got %d", len(expectedSequence), len(appender.Calls()))
	}

	for i, expectedMethod := range expectedSequence {
		if appender.Calls()[i].Method != expectedMethod {
			t.Errorf("call %d: expected %s, got %s", i, expectedMethod, appender.Calls()[i].Method)
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
		if appender.Calls()[i].RecordType != expectedType {
			t.Errorf("record %d: expected type %v, got %v", i, expectedType, appender.Calls()[i].RecordType)
		}
	}
}

// TestHappyPathWithoutFSync verifies the call sequence: BEGIN → ops → COMMIT → Flush (no FSync)
func TestHappyPathWithoutFSync(t *testing.T) {
	allocator := testutil.NewIDAllocator(200)
	appender := testutil.NewLogAppender()
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
	if len(appender.Calls()) != len(expectedSequence) {
		t.Errorf("expected %d calls, got %d", len(expectedSequence), len(appender.Calls()))
	}

	for i, expectedMethod := range expectedSequence {
		if appender.Calls()[i].Method != expectedMethod {
			t.Errorf("call %d: expected %s, got %s", i, expectedMethod, appender.Calls()[i].Method)
		}
	}

	// Verify FSync was not called
	for _, call := range appender.Calls() {
		if call.Method == "FSync" {
			t.Error("FSync should not have been called when FsyncOnCommit=false")
		}
	}
}

// TestFailOnOpAppend verifies: error returned; no COMMIT appended; no Flush/FSync
func TestFailOnOpAppend(t *testing.T) {
	allocator := testutil.NewIDAllocator(300)
	appender := testutil.NewLogAppender()
	appender.SetFailOnAppend(1) // Fail on second append (first operation)
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
	if len(appender.Calls()) != 1 {
		t.Errorf("expected 1 call (BEGIN only), got %d", len(appender.Calls()))
	}

	if appender.Calls()[0].RecordType != record.RecordTypeBeginTransaction {
		t.Errorf("expected BEGIN, got %v", appender.Calls()[0].RecordType)
	}

	// Verify no Flush or FSync were called
	for _, call := range appender.Calls() {
		if call.Method == "Flush" || call.Method == "FSync" {
			t.Errorf("expected no %s call on operation append failure", call.Method)
		}
	}

	_ = txnID
}

// TestFailOnCommitAppend verifies: error returned; no Flush/FSync
func TestFailOnCommitAppend(t *testing.T) {
	allocator := testutil.NewIDAllocator(400)
	appender := testutil.NewLogAppender()
	appender.SetFailOnAppend(4) // Fail on fifth append (COMMIT after BEGIN + 3 ops)
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
	for _, call := range appender.Calls() {
		if call.Method == "Append" {
			appendCount++
		}
	}

	if appendCount != 4 {
		t.Errorf("expected 4 appends (BEGIN + 3 ops), got %d", appendCount)
	}

	// Verify no COMMIT appended
	hasCommit := false
	for _, call := range appender.Calls() {
		if call.Method == "Append" && call.RecordType == record.RecordTypeCommitTransaction {
			hasCommit = true
		}
	}
	if hasCommit {
		t.Error("COMMIT should not have been appended on COMMIT append failure")
	}

	// Verify no Flush or FSync were called
	for _, call := range appender.Calls() {
		if call.Method == "Flush" || call.Method == "FSync" {
			t.Errorf("expected no %s call on COMMIT append failure", call.Method)
		}
	}

	_ = txnID
}

// TestFailOnFlush verifies: error returned; no FSync
func TestFailOnFlush(t *testing.T) {
	allocator := testutil.NewIDAllocator(500)
	appender := testutil.NewLogAppender()
	appender.SetFailOnFlush(true)
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
	for _, call := range appender.Calls() {
		if call.Method == "Append" {
			appendCount++
		}
	}

	if appendCount != 3 {
		t.Errorf("expected 3 appends (BEGIN + PUT + COMMIT), got %d", appendCount)
	}

	// Verify Flush was called but FSync was not
	flushCalled := false
	fsyncCalled := false
	for _, call := range appender.Calls() {
		if call.Method == "Flush" {
			flushCalled = true
		}
		if call.Method == "FSync" {
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
	allocator := testutil.NewIDAllocator(600)
	appender := testutil.NewLogAppender()
	appender.SetFailOnFSync(true)
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

	for _, call := range appender.Calls() {
		if call.Method == "Append" {
			appendCount++
		}
		if call.Method == "Flush" {
			flushCalled = true
		}
		if call.Method == "FSync" {
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
	if len(appender.Calls()) != len(expectedSequence) {
		t.Errorf("expected %d calls, got %d", len(expectedSequence), len(appender.Calls()))
	}

	_ = txnID
}
