package wal_test

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	tst "github.com/julianstephens/go-utils/tests"
	"github.com/julianstephens/waldb/internal/waldb/wal"
	"github.com/julianstephens/waldb/internal/waldb/wal/record"
)

// Helper function to create a temporary file for testing
func createTempSegmentFile(t *testing.T) *os.File {
	tempDir := t.TempDir()
	file, err := os.Create(filepath.Join(tempDir, "segment")) //nolint:gosec
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	return file
}

// createBeginPayload creates a properly formatted BeginTransaction payload
func createBeginPayload(txnID uint64) []byte {
	payload := make([]byte, record.TxnIdSize)
	binary.LittleEndian.PutUint64(payload, txnID)
	return payload
}

// createPutPayload creates a properly formatted PutOperation payload
func createPutPayload(txnID uint64, key, value []byte) []byte {
	size := record.TxnIdSize + record.PayloadHeaderSize + len(key) + record.PayloadHeaderSize + len(value)
	payload := make([]byte, size)
	off := 0

	binary.LittleEndian.PutUint64(payload[off:off+record.TxnIdSize], txnID)
	off += record.TxnIdSize

	binary.LittleEndian.PutUint32(payload[off:off+record.PayloadHeaderSize], uint32(len(key))) //nolint:gosec
	off += record.PayloadHeaderSize
	copy(payload[off:off+len(key)], key)
	off += len(key)

	binary.LittleEndian.PutUint32(payload[off:off+record.PayloadHeaderSize], uint32(len(value))) //nolint:gosec
	off += record.PayloadHeaderSize
	copy(payload[off:off+len(value)], value)

	return payload
}

// createDeletePayload creates a properly formatted DeleteOperation payload
func createDeletePayload(txnID uint64, key []byte) []byte {
	size := record.TxnIdSize + record.PayloadHeaderSize + len(key)
	payload := make([]byte, size)
	off := 0

	binary.LittleEndian.PutUint64(payload[off:off+record.TxnIdSize], txnID)
	off += record.TxnIdSize

	binary.LittleEndian.PutUint32(payload[off:off+record.PayloadHeaderSize], uint32(len(key))) //nolint:gosec
	off += record.PayloadHeaderSize
	copy(payload[off:off+len(key)], key)

	return payload
}

// TestNewSegmentAppenderValid tests creating a SegmentAppender with a valid file
func TestNewSegmentAppenderValid(t *testing.T) {
	file := createTempSegmentFile(t)
	defer file.Close() //nolint:errcheck

	writer, err := wal.NewSegmentAppender(file)
	tst.RequireNoError(t, err)
	tst.AssertNotNil(t, writer, "expected non-nil SegmentAppender")
	defer writer.Close() //nolint:errcheck
}

// TestNewSegmentAppenderNilFile tests creating a SegmentAppender with nil file
func TestNewSegmentAppenderNilFile(t *testing.T) {
	writer, err := wal.NewSegmentAppender(nil)
	if err == nil {
		t.Fatal("expected error for nil file")
	}
	if writer != nil {
		t.Fatal("expected nil SegmentAppender")
	}
}

// TestAppendSingleRecord tests appending a single record
func TestAppendSingleRecord(t *testing.T) {
	file := createTempSegmentFile(t)
	defer file.Close() //nolint:errcheck

	writer, err := wal.NewSegmentAppender(file)
	tst.RequireNoError(t, err)
	defer writer.Close() //nolint:errcheck

	payload := createBeginPayload(1)
	offset, err := writer.Append(record.RecordTypeBeginTransaction, payload)
	tst.RequireNoError(t, err)

	tst.RequireDeepEqual(t, offset, int64(0))
}

// TestAppendMultipleRecords tests appending multiple records
func TestAppendMultipleRecords(t *testing.T) {
	file := createTempSegmentFile(t)
	defer file.Close() //nolint:errcheck

	writer, err := wal.NewSegmentAppender(file)
	tst.RequireNoError(t, err)
	defer writer.Close() //nolint:errcheck

	offsets := make([]int64, 0)
	for i := uint64(1); i <= 3; i++ {
		payload := createBeginPayload(i)
		offset, err := writer.Append(record.RecordTypeBeginTransaction, payload)
		tst.RequireNoError(t, err)
		offsets = append(offsets, offset)
	}

	// Verify offsets are increasing
	for i := 1; i < len(offsets); i++ {
		tst.AssertTrue(t, offsets[i] > offsets[i-1], "offset should be increasing")
	}
}

// TestAppendDifferentRecordTypes tests appending records of different types
func TestAppendDifferentRecordTypes(t *testing.T) {
	file := createTempSegmentFile(t)
	defer file.Close() //nolint:errcheck

	writer, err := wal.NewSegmentAppender(file)
	tst.RequireNoError(t, err)
	defer writer.Close() //nolint:errcheck

	// Test begin transaction
	beginPayload := createBeginPayload(1)
	_, err = writer.Append(record.RecordTypeBeginTransaction, beginPayload)
	tst.RequireNoError(t, err)
	if err != nil {
		t.Fatalf("unexpected error appending begin record: %v", err)
	}

	// Test put operation
	putPayload := createPutPayload(1, []byte("key"), []byte("value"))
	_, err = writer.Append(record.RecordTypePutOperation, putPayload)
	if err != nil {
		t.Fatalf("unexpected error appending put record: %v", err)
	}

	// Test delete operation
	deletePayload := createDeletePayload(1, []byte("key"))
	_, err = writer.Append(record.RecordTypeDeleteOperation, deletePayload)
	if err != nil {
		t.Fatalf("unexpected error appending delete record: %v", err)
	}

	// Test commit transaction
	commitPayload := createBeginPayload(1)
	_, err = writer.Append(record.RecordTypeCommitTransaction, commitPayload)
	if err != nil {
		t.Fatalf("unexpected error appending commit record: %v", err)
	}
}

// TestAppendMinimalPayloads tests appending records with minimal valid payloads
func TestAppendMinimalPayloads(t *testing.T) {
	file := createTempSegmentFile(t)
	defer file.Close() //nolint:errcheck

	writer, err := wal.NewSegmentAppender(file)
	if err != nil {
		t.Fatalf("unexpected error creating writer: %v", err)
	}
	defer writer.Close() //nolint:errcheck

	// Begin with minimal payload (just txn ID)
	payload := createBeginPayload(1)
	_, err = writer.Append(record.RecordTypeBeginTransaction, payload)
	if err != nil {
		t.Fatalf("unexpected error appending begin: %v", err)
	}
}

// TestAppendLargePayload tests appending a record with a large payload
func TestAppendLargePayload(t *testing.T) {
	file := createTempSegmentFile(t)
	defer file.Close() //nolint:errcheck

	writer, err := wal.NewSegmentAppender(file)
	if err != nil {
		t.Fatalf("unexpected error creating writer: %v", err)
	}
	defer writer.Close() //nolint:errcheck

	// Create a large value (1MB)
	largeValue := make([]byte, 1024*1024)
	for i := range largeValue {
		largeValue[i] = byte(i % 256)
	}

	payload := createPutPayload(1, []byte("key"), largeValue)
	_, err = writer.Append(record.RecordTypePutOperation, payload)
	if err != nil {
		t.Fatalf("unexpected error appending large payload: %v", err)
	}
}

// TestAppendAfterClose tests appending after closing the writer
func TestAppendAfterClose(t *testing.T) {
	file := createTempSegmentFile(t)
	defer file.Close() //nolint:errcheck

	writer, err := wal.NewSegmentAppender(file)
	if err != nil {
		t.Fatalf("unexpected error creating writer: %v", err)
	}

	if err := writer.Close(); err != nil {
		t.Fatalf("unexpected error closing writer: %v", err)
	}

	payload := createBeginPayload(1)
	_, err = writer.Append(record.RecordTypeBeginTransaction, payload)
	if err == nil {
		t.Fatal("expected error appending after close")
	}
}

// TestFlushSingleRecord tests flushing after appending a record
func TestFlushSingleRecord(t *testing.T) {
	file := createTempSegmentFile(t)
	defer file.Close() //nolint:errcheck

	writer, err := wal.NewSegmentAppender(file)
	if err != nil {
		t.Fatalf("unexpected error creating writer: %v", err)
	}
	defer writer.Close() //nolint:errcheck

	payload := createBeginPayload(1)
	_, err = writer.Append(record.RecordTypeBeginTransaction, payload)
	if err != nil {
		t.Fatalf("unexpected error appending: %v", err)
	}

	if err := writer.Flush(); err != nil {
		t.Fatalf("unexpected error flushing: %v", err)
	}
}

// TestFlushAfterClose tests flushing after closing
func TestFlushAfterClose(t *testing.T) {
	file := createTempSegmentFile(t)
	defer file.Close() //nolint:errcheck

	writer, err := wal.NewSegmentAppender(file)
	if err != nil {
		t.Fatalf("unexpected error creating writer: %v", err)
	}

	if err := writer.Close(); err != nil {
		t.Fatalf("unexpected error closing: %v", err)
	}

	err = writer.Flush()
	if err == nil {
		t.Fatal("expected error flushing after close")
	}
}

// TestFSyncSingleRecord tests fsyncing after appending a record
func TestFSyncSingleRecord(t *testing.T) {
	file := createTempSegmentFile(t)
	defer file.Close() //nolint:errcheck

	writer, err := wal.NewSegmentAppender(file)
	if err != nil {
		t.Fatalf("unexpected error creating writer: %v", err)
	}
	defer writer.Close() //nolint:errcheck

	payload := createBeginPayload(1)
	_, err = writer.Append(record.RecordTypeBeginTransaction, payload)
	if err != nil {
		t.Fatalf("unexpected error appending: %v", err)
	}

	if err := writer.FSync(); err != nil {
		t.Fatalf("unexpected error fsyncing: %v", err)
	}
}

// TestFSyncAfterClose tests fsyncing after closing
func TestFSyncAfterClose(t *testing.T) {
	file := createTempSegmentFile(t)
	defer file.Close() //nolint:errcheck

	writer, err := wal.NewSegmentAppender(file)
	if err != nil {
		t.Fatalf("unexpected error creating writer: %v", err)
	}

	if err := writer.Close(); err != nil {
		t.Fatalf("unexpected error closing: %v", err)
	}

	err = writer.FSync()
	if err == nil {
		t.Fatal("expected error fsyncing after close")
	}
}

// TestCloseMultipleTimes tests closing the writer multiple times
func TestCloseMultipleTimes(t *testing.T) {
	file := createTempSegmentFile(t)
	defer file.Close() //nolint:errcheck

	writer, err := wal.NewSegmentAppender(file)
	if err != nil {
		t.Fatalf("unexpected error creating writer: %v", err)
	}

	// First close should succeed
	if err := writer.Close(); err != nil {
		t.Fatalf("unexpected error on first close: %v", err)
	}

	// Second close should not error (idempotent)
	if err := writer.Close(); err != nil {
		t.Fatalf("unexpected error on second close: %v", err)
	}
}

// TestExistingSegmentFile tests creating a writer from an existing segment file
func TestExistingSegmentFile(t *testing.T) {
	file := createTempSegmentFile(t)
	defer file.Close() //nolint:errcheck

	// Write some initial data
	payload := createBeginPayload(1)
	encoded, err := record.EncodeFrame(record.RecordTypeBeginTransaction, payload)
	if err != nil {
		t.Fatalf("unexpected error encoding: %v", err)
	}

	if _, err := file.Write(encoded); err != nil {
		t.Fatalf("unexpected error writing initial data: %v", err)
	}

	_ = file.Close() //nolint:gosec

	// Reopen the file and create a writer
	file, err = os.OpenFile(file.Name(), os.O_RDWR, 0o600) //nolint:gosec
	if err != nil {
		t.Fatalf("unexpected error reopening file: %v", err)
	}
	defer file.Close() //nolint:errcheck

	writer, err := wal.NewSegmentAppender(file)
	if err != nil {
		t.Fatalf("unexpected error creating writer: %v", err)
	}
	defer writer.Close() //nolint:errcheck

	// Verify the new writer starts after the existing data
	expectedOffset := int64(len(encoded))

	newPayload := createBeginPayload(2)
	offset, err := writer.Append(record.RecordTypeCommitTransaction, newPayload)
	if err != nil {
		t.Fatalf("unexpected error appending: %v", err)
	}

	if offset != expectedOffset {
		t.Errorf("expected offset %d, got %d", expectedOffset, offset)
	}
}

// TestAppendMultipleAndFlush tests appending multiple records and flushing
func TestAppendMultipleAndFlush(t *testing.T) {
	file := createTempSegmentFile(t)
	defer file.Close() //nolint:errcheck

	writer, err := wal.NewSegmentAppender(file)
	if err != nil {
		t.Fatalf("unexpected error creating writer: %v", err)
	}
	defer writer.Close() //nolint:errcheck

	// Append 10 records
	for i := uint64(1); i <= 10; i++ {
		payload := createBeginPayload(i)
		_, err := writer.Append(record.RecordTypeBeginTransaction, payload)
		if err != nil {
			t.Fatalf("record %d: unexpected error: %v", i, err)
		}
	}

	// Flush all records
	if err := writer.Flush(); err != nil {
		t.Fatalf("unexpected error flushing: %v", err)
	}

	// Verify file size is non-zero
	if err := writer.Close(); err != nil {
		t.Fatalf("unexpected error closing: %v", err)
	}

	fileInfo, err := os.Stat(file.Name())
	if err != nil {
		t.Fatalf("unexpected error stating file: %v", err)
	}

	if fileInfo.Size() == 0 {
		t.Fatal("expected non-zero file size after flush")
	}
}

// TestAppendAndFSync tests appending and then fsyncing
func TestAppendAndFSync(t *testing.T) {
	file := createTempSegmentFile(t)
	defer file.Close() //nolint:errcheck

	writer, err := wal.NewSegmentAppender(file)
	if err != nil {
		t.Fatalf("unexpected error creating writer: %v", err)
	}
	defer writer.Close() //nolint:errcheck

	payload := createBeginPayload(1)
	_, err = writer.Append(record.RecordTypeBeginTransaction, payload)
	if err != nil {
		t.Fatalf("unexpected error appending: %v", err)
	}

	if err := writer.FSync(); err != nil {
		t.Fatalf("unexpected error fsyncing: %v", err)
	}

	// File should be readable after FSync
	if err := writer.Close(); err != nil {
		t.Fatalf("unexpected error closing: %v", err)
	}

	fileInfo, err := os.Stat(file.Name())
	if err != nil {
		t.Fatalf("unexpected error stating file: %v", err)
	}

	if fileInfo.Size() == 0 {
		t.Fatal("expected non-zero file size after fsync")
	}
}

// TestConcurrentAppends tests appending from multiple goroutines
func TestConcurrentAppends(t *testing.T) {
	file := createTempSegmentFile(t)
	defer file.Close() //nolint:errcheck

	writer, err := wal.NewSegmentAppender(file)
	if err != nil {
		t.Fatalf("unexpected error creating writer: %v", err)
	}
	defer writer.Close() //nolint:errcheck

	numGoroutines := 10
	numRecordsPerGoroutine := 10
	done := make(chan bool, numGoroutines)
	errors := make(chan error, numGoroutines*numRecordsPerGoroutine)

	// Spawn multiple goroutines appending records
	for g := 0; g < numGoroutines; g++ {
		go func(goroutineID int) {
			for i := 0; i < numRecordsPerGoroutine; i++ {
				txnID := uint64(goroutineID*numRecordsPerGoroutine + i + 1) //nolint:gosec
				payload := createBeginPayload(txnID)
				_, err := writer.Append(record.RecordTypeBeginTransaction, payload)
				if err != nil {
					errors <- err
				}
			}
			done <- true
		}(g)
	}

	// Wait for all goroutines to complete
	for i := 0; i < numGoroutines; i++ {
		<-done
	}

	// Check for any errors
	close(errors)
	for err := range errors {
		if err != nil {
			t.Fatalf("unexpected error during concurrent append: %v", err)
		}
	}

	// Flush and close
	if err := writer.Flush(); err != nil {
		t.Fatalf("unexpected error flushing: %v", err)
	}

	if err := writer.Close(); err != nil {
		t.Fatalf("unexpected error closing: %v", err)
	}

	// Verify file is not empty
	fileInfo, err := os.Stat(file.Name())
	if err != nil {
		t.Fatalf("unexpected error stating file: %v", err)
	}

	expectedRecords := numGoroutines * numRecordsPerGoroutine
	if fileInfo.Size() == 0 {
		t.Fatalf("expected non-zero file size for %d records", expectedRecords)
	}
}

// TestWrittenDataReadability tests that written data can be read back
func TestWrittenDataReadability(t *testing.T) {
	file := createTempSegmentFile(t)
	defer file.Close() //nolint:errcheck

	writer, err := wal.NewSegmentAppender(file)
	if err != nil {
		t.Fatalf("unexpected error creating writer: %v", err)
	}

	payload := createBeginPayload(1)
	_, err = writer.Append(record.RecordTypeBeginTransaction, payload)
	if err != nil {
		t.Fatalf("unexpected error appending: %v", err)
	}

	if err := writer.FSync(); err != nil {
		t.Fatalf("unexpected error fsyncing: %v", err)
	}

	if err := writer.Close(); err != nil {
		t.Fatalf("unexpected error closing: %v", err)
	}

	// Read the file back
	data, err := os.ReadFile(file.Name())
	if err != nil {
		t.Fatalf("unexpected error reading file: %v", err)
	}

	if len(data) == 0 {
		t.Fatal("expected non-empty file")
	}

	// The data should be readable as a framed record
	reader := bytes.NewReader(data)

	// Read header (first 4 bytes contain the record length)
	headerBuf := make([]byte, record.RecordHeaderSize)
	if _, err := io.ReadFull(reader, headerBuf); err != nil {
		t.Fatalf("unexpected error reading header: %v", err)
	}
}

// TestAppendInvalidRecordType tests appending with an invalid record type
func TestAppendInvalidRecordType(t *testing.T) {
	file := createTempSegmentFile(t)
	defer file.Close() //nolint:errcheck

	writer, err := wal.NewSegmentAppender(file)
	if err != nil {
		t.Fatalf("unexpected error creating writer: %v", err)
	}
	defer writer.Close() //nolint:errcheck

	// Use an invalid record type with payload of wrong length
	payload := []byte("invalid")
	_, err = writer.Append(record.RecordType(255), payload)
	if err == nil {
		t.Fatal("expected error for invalid record type")
	}
}

// TestAppendBeginInvalidPayloadSize tests appending Begin with wrong payload size
func TestAppendBeginInvalidPayloadSize(t *testing.T) {
	file := createTempSegmentFile(t)
	defer file.Close() //nolint:errcheck

	writer, err := wal.NewSegmentAppender(file)
	if err != nil {
		t.Fatalf("unexpected error creating writer: %v", err)
	}
	defer writer.Close() //nolint:errcheck

	// Begin requires exactly TxnIdSize bytes
	invalidPayload := make([]byte, 4) // Wrong size
	_, err = writer.Append(record.RecordTypeBeginTransaction, invalidPayload)
	if err == nil {
		t.Fatal("expected error for invalid Begin payload size")
	}
}

// TestAppendCommitInvalidPayloadSize tests appending Commit with wrong payload size
func TestAppendCommitInvalidPayloadSize(t *testing.T) {
	file := createTempSegmentFile(t)
	defer file.Close() //nolint:errcheck

	writer, err := wal.NewSegmentAppender(file)
	if err != nil {
		t.Fatalf("unexpected error creating writer: %v", err)
	}
	defer writer.Close() //nolint:errcheck

	// Commit requires exactly TxnIdSize bytes
	invalidPayload := make([]byte, 10) // Wrong size
	_, err = writer.Append(record.RecordTypeCommitTransaction, invalidPayload)
	if err == nil {
		t.Fatal("expected error for invalid Commit payload size")
	}
}

// TestAppendPutInsufficientPayload tests appending Put with too-small payload
func TestAppendPutInsufficientPayload(t *testing.T) {
	file := createTempSegmentFile(t)
	defer file.Close() //nolint:errcheck

	writer, err := wal.NewSegmentAppender(file)
	if err != nil {
		t.Fatalf("unexpected error creating writer: %v", err)
	}
	defer writer.Close() //nolint:errcheck

	// Put requires at least TxnIdSize + 2*PayloadHeaderSize
	tooSmallPayload := make([]byte, record.TxnIdSize)
	_, err = writer.Append(record.RecordTypePutOperation, tooSmallPayload)
	if err == nil {
		t.Fatal("expected error for insufficient Put payload")
	}
}

// TestAppendDeleteInsufficientPayload tests appending Delete with too-small payload
func TestAppendDeleteInsufficientPayload(t *testing.T) {
	file := createTempSegmentFile(t)
	defer file.Close() //nolint:errcheck

	writer, err := wal.NewSegmentAppender(file)
	if err != nil {
		t.Fatalf("unexpected error creating writer: %v", err)
	}
	defer writer.Close() //nolint:errcheck

	// Delete requires at least TxnIdSize + PayloadHeaderSize
	tooSmallPayload := make([]byte, record.TxnIdSize)
	_, err = writer.Append(record.RecordTypeDeleteOperation, tooSmallPayload)
	if err == nil {
		t.Fatal("expected error for insufficient Delete payload")
	}
}

// TestAppendPutPayloadTooLarge tests appending with payload exceeding MaxRecordSize
func TestAppendPutPayloadTooLarge(t *testing.T) {
	file := createTempSegmentFile(t)
	defer file.Close() //nolint:errcheck

	writer, err := wal.NewSegmentAppender(file)
	if err != nil {
		t.Fatalf("unexpected error creating writer: %v", err)
	}
	defer writer.Close() //nolint:errcheck

	// Create payload larger than MaxRecordSize
	oversizeValue := make([]byte, record.MaxRecordSize+1)
	payload := createPutPayload(1, []byte("key"), oversizeValue)

	_, err = writer.Append(record.RecordTypePutOperation, payload)
	if err == nil {
		t.Fatal("expected error for oversized payload")
	}
}

// TestAppendOversizeDeleteKey tests appending Delete with key exceeding MaxKeySize
func TestAppendOversizeDeleteKey(t *testing.T) {
	file := createTempSegmentFile(t)
	defer file.Close() //nolint:errcheck

	writer, err := wal.NewSegmentAppender(file)
	if err != nil {
		t.Fatalf("unexpected error creating writer: %v", err)
	}
	defer writer.Close() //nolint:errcheck

	// Create key larger than MaxKeySize
	oversizeKey := make([]byte, record.MaxKeySize+1)
	payload := createDeletePayload(1, oversizeKey)

	_, err = writer.Append(record.RecordTypeDeleteOperation, payload)
	if err == nil {
		t.Fatal("expected error for oversized key in delete")
	}
}

// TestAppendReturnError checks that Append returns SegmentWriteError on validation failure
func TestAppendReturnError(t *testing.T) {
	file := createTempSegmentFile(t)
	defer file.Close() //nolint:errcheck

	writer, err := wal.NewSegmentAppender(file)
	if err != nil {
		t.Fatalf("unexpected error creating writer: %v", err)
	}
	defer writer.Close() //nolint:errcheck

	// Invalid record type
	invalidPayload := []byte("test")
	_, err = writer.Append(record.RecordType(255), invalidPayload)

	// Should return an error
	if err == nil {
		t.Fatal("expected error")
	}

	// Check that it's a SegmentWriteError
	var segErr *wal.SegmentWriteError
	tst.AssertTrue(t, errors.As(err, &segErr), "expected SegmentWriteError")

	// Check the error type is ErrInvalidRecord
	tst.RequireDeepEqual(t, segErr.Err, wal.ErrInvalidRecord)

	// Check that Offset is set
	tst.RequireDeepEqual(t, segErr.Offset, int64(0))

	// Check that RecordType is set
	tst.RequireDeepEqual(t, segErr.RecordType, record.RecordType(255))
}

// TestFlushOnClosedWriter checks that Flush returns error on closed writer
func TestFlushOnClosedWriter(t *testing.T) {
	file := createTempSegmentFile(t)
	defer file.Close() //nolint:errcheck,gosec

	writer, err := wal.NewSegmentAppender(file)
	tst.RequireNoError(t, err)

	// Close the writer
	tst.RequireNoError(t, writer.Close())

	// Flush should return an error
	err = writer.Flush()
	if err == nil {
		t.Fatal("expected error from Flush on closed writer")
	}

	// Check that it's a SegmentWriteError
	var segErr *wal.SegmentWriteError
	tst.AssertTrue(t, errors.As(err, &segErr), "expected SegmentWriteError")
}

// TestFSyncReturnError checks that FSync returns SegmentWriteError
func TestFSyncReturnError(t *testing.T) {
	file := createTempSegmentFile(t)
	defer file.Close() //nolint:errcheck

	writer, err := wal.NewSegmentAppender(file)
	tst.RequireNoError(t, err)
	defer writer.Close() //nolint:errcheck

	// Close the underlying file to cause FSync to fail
	file.Close() //nolint:errcheck,gosec

	err = writer.FSync()

	// Should return an error
	if err == nil {
		t.Fatal("expected error from FSync on closed file")
	}

	// Check that it's a SegmentWriteError
	var segErr *wal.SegmentWriteError
	tst.AssertTrue(t, errors.As(err, &segErr), "expected SegmentWriteError")
}

// TestCloseReturnError checks that Close returns SegmentWriteError on failure
func TestCloseReturnError(t *testing.T) {
	file := createTempSegmentFile(t)
	defer file.Close() //nolint:errcheck

	writer, err := wal.NewSegmentAppender(file)
	tst.RequireNoError(t, err)

	// Close the underlying file to cause Close to fail
	file.Close() //nolint:errcheck,gosec

	err = writer.Close()

	// Should return error the first time
	if err == nil {
		t.Fatal("expected error from Close on closed file")
	}

	err = writer.Close()

	// Second close should not error
	if err != nil {
		t.Fatal("expected no error on second Close")
	}
}

// TestSegmentWriteErrorInterface tests SegmentWriteError implements error interface
func TestSegmentWriteErrorInterface(t *testing.T) {
	err := &wal.SegmentWriteError{
		Err:        wal.ErrInvalidRecord,
		Offset:     100,
		RecordType: record.RecordTypeBeginTransaction,
	}

	// Should implement error interface
	var _ error = err

	// Error() should return string
	errMsg := err.Error()
	if len(errMsg) == 0 {
		t.Fatal("expected non-empty error message")
	}

	// Unwrap should return the underlying error
	unwrapped := err.Unwrap()
	if unwrapped != wal.ErrInvalidRecord {
		t.Errorf("expected ErrInvalidRecord, got %v", unwrapped)
	}
}

// TestRoundTripDecodeSingleRecord tests encoding and decoding a single record
func TestRoundTripDecodeSingleRecord(t *testing.T) {
	file := createTempSegmentFile(t)
	defer file.Close() //nolint:errcheck

	writer, err := wal.NewSegmentAppender(file)
	tst.RequireNoError(t, err)
	defer writer.Close() //nolint:errcheck

	// Append a known record
	payload := createBeginPayload(42)
	_, err = writer.Append(record.RecordTypeBeginTransaction, payload)
	tst.RequireNoError(t, err)

	// Flush and close
	tst.RequireNoError(t, writer.Flush())
	tst.RequireNoError(t, writer.Close())

	// Read file back
	data, err := os.ReadFile(file.Name())
	tst.RequireNoError(t, err)
	tst.AssertTrue(t, len(data) > 0, "expected non-empty file")

	// Walk the buffer: read length, slice record, decode
	buf := bytes.NewReader(data)
	lengthBuf := make([]byte, 4)
	_, err = io.ReadFull(buf, lengthBuf)
	tst.RequireNoError(t, err)

	declaredLen := binary.LittleEndian.Uint32(lengthBuf)
	// Record format: 4 bytes length + payload + 4 bytes CRC
	recordBytes := make([]byte, int(declaredLen)+4)
	_, err = io.ReadFull(buf, recordBytes)
	tst.RequireNoError(t, err)

	// Decode the record
	decoded, err := record.DecodeFrame(append(lengthBuf, recordBytes...))
	tst.RequireNoError(t, err)

	// Verify type and payload match
	tst.RequireDeepEqual(t, decoded.Record.Type, record.RecordTypeBeginTransaction)
	tst.RequireDeepEqual(t, decoded.Record.Payload, payload)

	// Verify EOF (no trailing bytes)
	eofBuf := make([]byte, 1)
	n, err := buf.Read(eofBuf)
	tst.RequireDeepEqual(t, n, 0)
	tst.RequireDeepEqual(t, err, io.EOF)
}

// TestRoundTripDecodeMultipleRecords tests encoding and decoding multiple records
func TestRoundTripDecodeMultipleRecords(t *testing.T) {
	file := createTempSegmentFile(t)
	defer file.Close() //nolint:errcheck

	writer, err := wal.NewSegmentAppender(file)
	tst.RequireNoError(t, err)
	defer writer.Close() //nolint:errcheck

	// Create test records with known data
	testRecords := []struct {
		recordType record.RecordType
		payload    []byte
	}{
		{record.RecordTypeBeginTransaction, createBeginPayload(1)},
		{record.RecordTypePutOperation, createPutPayload(1, []byte("key1"), []byte("value1"))},
		{record.RecordTypeDeleteOperation, createDeletePayload(1, []byte("key1"))},
		{record.RecordTypeCommitTransaction, createBeginPayload(1)},
		{record.RecordTypeBeginTransaction, createBeginPayload(2)},
		{record.RecordTypePutOperation, createPutPayload(2, []byte("key2"), []byte("value2"))},
		{record.RecordTypeCommitTransaction, createBeginPayload(2)},
	}

	// Append all records
	for _, tr := range testRecords {
		_, err := writer.Append(tr.recordType, tr.payload)
		tst.RequireNoError(t, err)
	}

	// Flush and close
	tst.RequireNoError(t, writer.Flush())
	tst.RequireNoError(t, writer.Close())

	// Read file back
	data, err := os.ReadFile(file.Name())
	tst.RequireNoError(t, err)
	tst.AssertTrue(t, len(data) > 0, "expected non-empty file")

	// Decode all records and verify
	buf := bytes.NewReader(data)
	decodedCount := 0

	for {
		lengthBuf := make([]byte, 4)
		n, err := buf.Read(lengthBuf)
		if err == io.EOF {
			break
		}
		tst.RequireNoError(t, err)
		tst.RequireDeepEqual(t, n, 4)

		declaredLen := binary.LittleEndian.Uint32(lengthBuf)
		recordBytes := make([]byte, int(declaredLen)+4)
		_, err = io.ReadFull(buf, recordBytes)
		tst.RequireNoError(t, err)

		// Decode the record
		decoded, err := record.DecodeFrame(append(lengthBuf, recordBytes...))
		tst.RequireNoError(t, err)

		// Verify matches expected
		tst.RequireDeepEqual(t, decoded.Record.Type, testRecords[decodedCount].recordType)
		tst.RequireDeepEqual(t, decoded.Record.Payload, testRecords[decodedCount].payload)

		decodedCount++
	}

	// Verify we decoded all records
	tst.RequireDeepEqual(t, decodedCount, len(testRecords))
}

// TestOffsetsAreExact verifies offset[i+1] == offset[i] + sizeOfEncodedRecord(i)
func TestOffsetsAreExact(t *testing.T) {
	file := createTempSegmentFile(t)
	defer file.Close() //nolint:errcheck

	writer, err := wal.NewSegmentAppender(file)
	tst.RequireNoError(t, err)
	defer writer.Close() //nolint:errcheck

	// Create records with varying payload sizes
	testRecords := []struct {
		recordType record.RecordType
		payload    []byte
	}{
		{record.RecordTypeBeginTransaction, createBeginPayload(1)},
		{record.RecordTypePutOperation, createPutPayload(1, []byte("short"), []byte("value"))},
		{
			record.RecordTypePutOperation,
			createPutPayload(2, []byte("longer_key_name"), []byte("much longer value content")),
		},
		{record.RecordTypeDeleteOperation, createDeletePayload(3, []byte("key"))},
		{record.RecordTypeCommitTransaction, createBeginPayload(3)},
	}

	// Track offsets and encoded sizes
	offsets := make([]int64, 0)
	encodedSizes := make([]int, 0)

	for _, tr := range testRecords {
		offset, err := writer.Append(tr.recordType, tr.payload)
		tst.RequireNoError(t, err)
		offsets = append(offsets, offset)

		// Calculate what the encoded size will be
		encoded, err := record.EncodeFrame(tr.recordType, tr.payload)
		tst.RequireNoError(t, err)
		encodedSizes = append(encodedSizes, len(encoded))
	}

	// Verify offsets are exact (not just increasing)
	// offset[i+1] should equal offset[i] + sizeOfEncodedRecord[i]
	for i := 0; i < len(offsets)-1; i++ {
		expectedNextOffset := offsets[i] + int64(encodedSizes[i])
		tst.RequireDeepEqual(t, offsets[i+1], expectedNextOffset)
	}
}

// TestAppendDoesNotFlush verifies that Append doesn't implicitly flush
func TestAppendDoesNotFlush(t *testing.T) {
	file := createTempSegmentFile(t)
	defer file.Close() //nolint:errcheck

	writer, err := wal.NewSegmentAppender(file)
	tst.RequireNoError(t, err)
	defer writer.Close() //nolint:errcheck

	// Get initial file size
	stat1, err := os.Stat(file.Name())
	tst.RequireNoError(t, err)
	initialSize := stat1.Size()

	// Append a record without flushing
	payload := createPutPayload(1, []byte("key"), []byte("significant value data"))
	_, err = writer.Append(record.RecordTypePutOperation, payload)
	tst.RequireNoError(t, err)

	// File size should still be the same (buffered)
	stat2, err := os.Stat(file.Name())
	tst.RequireNoError(t, err)
	tst.RequireDeepEqual(t, stat2.Size(), initialSize)

	// Now flush
	tst.RequireNoError(t, writer.Flush())

	// File size should now be larger
	stat3, err := os.Stat(file.Name())
	tst.RequireNoError(t, err)
	tst.AssertTrue(t, stat3.Size() > initialSize, "expected file size to increase after flush")
}

// TestConcurrentAppendsConcurrencyInvariant tests that concurrent appends produce decodable records
func TestConcurrentAppendsConcurrencyInvariant(t *testing.T) {
	file := createTempSegmentFile(t)
	defer file.Close() //nolint:errcheck

	writer, err := wal.NewSegmentAppender(file)
	tst.RequireNoError(t, err)
	defer writer.Close() //nolint:errcheck

	numGoroutines := 10
	numRecordsPerGoroutine := 10
	done := make(chan bool, numGoroutines)
	errors := make(chan error, numGoroutines*numRecordsPerGoroutine)

	// Spawn multiple goroutines appending records
	for g := 0; g < numGoroutines; g++ {
		go func(goroutineID int) {
			for i := 0; i < numRecordsPerGoroutine; i++ {
				txnID := uint64(goroutineID*numRecordsPerGoroutine + i + 1) //nolint:gosec
				payload := createBeginPayload(txnID)
				_, err := writer.Append(record.RecordTypeBeginTransaction, payload)
				if err != nil {
					errors <- err
				}
			}
			done <- true
		}(g)
	}

	// Wait for all goroutines to complete
	for i := 0; i < numGoroutines; i++ {
		<-done
	}

	// Check for any errors
	close(errors)
	for err := range errors {
		tst.RequireNoError(t, err)
	}

	// Flush and close
	tst.RequireNoError(t, writer.Flush())
	tst.RequireNoError(t, writer.Close())

	// Read file back
	data, err := os.ReadFile(file.Name())
	tst.RequireNoError(t, err)

	// Decode all records and count
	buf := bytes.NewReader(data)
	decodedCount := 0

	for {
		lengthBuf := make([]byte, 4)
		n, err := buf.Read(lengthBuf)
		if err == io.EOF {
			break
		}
		tst.RequireNoError(t, err)
		if n != 4 {
			break
		}

		declaredLen := binary.LittleEndian.Uint32(lengthBuf)
		recordBytes := make([]byte, int(declaredLen)+4)
		_, err = io.ReadFull(buf, recordBytes)
		if err != nil {
			break
		}

		// Decode the record (ensures no corruption)
		_, err = record.DecodeFrame(append(lengthBuf, recordBytes...))
		tst.RequireNoError(t, err)

		decodedCount++
	}

	// Verify all records were decodable and count matches
	expectedRecords := numGoroutines * numRecordsPerGoroutine
	tst.RequireDeepEqual(t, decodedCount, expectedRecords)
}
