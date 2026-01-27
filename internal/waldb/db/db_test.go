package db_test

import (
	"errors"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"testing"

	tst "github.com/julianstephens/go-utils/tests"
	"github.com/julianstephens/waldb/internal/logger"
	"github.com/julianstephens/waldb/internal/testutil"
	waldb_db "github.com/julianstephens/waldb/internal/waldb/db"
	"github.com/julianstephens/waldb/internal/waldb/txn"
	"github.com/julianstephens/waldb/internal/waldb/wal/record"
)

// setupTestDB is a wrapper around testutil.SetupTestDB for backward compatibility
func setupTestDB(t *testing.T, dbPath string) {
	testutil.SetupTestDB(t, dbPath)
}

func TestOpenRequiresManifest(t *testing.T) {
	dbPath := t.TempDir()

	// Don't setup WAL dir, only try to open without manifest
	// Open should fail when manifest doesn't exist
	_, err := waldb_db.Open(dbPath, logger.NoOpLogger{})
	tst.AssertNotNil(t, err, "expected error when manifest does not exist")
}

func TestOpenWithExistingManifest(t *testing.T) {
	dbPath := t.TempDir()
	setupTestDB(t, dbPath)

	// Open should succeed with existing manifest
	db, err := waldb_db.Open(dbPath, logger.NoOpLogger{})
	tst.RequireNoError(t, err)
	defer func() {
		_ = db.Close()
	}()

	// Verify manifest exists
	_, err = os.Stat(dbPath + "/MANIFEST.json")
	tst.RequireNoError(t, err)
}

func TestOpenWithLogger(t *testing.T) {
	dbPath := t.TempDir()
	setupTestDB(t, dbPath)

	db, err := waldb_db.Open(dbPath, logger.NoOpLogger{})
	tst.RequireNoError(t, err)
	defer func() {
		_ = db.Close()
	}()

	tst.AssertFalse(t, db.IsClosed(), "expected database to be open")

	err = db.Close()
	tst.RequireNoError(t, err)

	tst.AssertTrue(t, db.IsClosed(), "expected database to be closed")

	// Test double close
	err = db.Close()
	tst.AssertNil(t, err, "expected no error on double close")
}

func TestOpenEmptyPath(t *testing.T) {
	_, err := waldb_db.Open("", logger.NoOpLogger{})
	tst.AssertNotNil(t, err, "expected error for empty path")
}

func TestCommitOperations_TableDriven(t *testing.T) {
	testCases := []struct {
		name        string
		setupBatch  func() *txn.Batch
		expectError bool
	}{
		{
			name: "ValidBatch",
			setupBatch: func() *txn.Batch {
				batch := txn.NewBatch()
				batch.Put([]byte("key1"), []byte("value1"))
				return batch
			},
			expectError: false,
		},
		{
			name: "MultiplePuts",
			setupBatch: func() *txn.Batch {
				batch := txn.NewBatch()
				batch.Put([]byte("key1"), []byte("value1"))
				batch.Put([]byte("key2"), []byte("value2"))
				batch.Put([]byte("key3"), []byte("value3"))
				return batch
			},
			expectError: false,
		},
		{
			name: "EmptyBatch",
			setupBatch: func() *txn.Batch {
				return txn.NewBatch()
			},
			expectError: true,
		},
		{
			name: "InvalidKeyNil",
			setupBatch: func() *txn.Batch {
				batch := txn.NewBatch()
				batch.Put(nil, []byte("value"))
				return batch
			},
			expectError: true,
		},
		{
			name: "OversizeKey",
			setupBatch: func() *txn.Batch {
				batch := txn.NewBatch()
				oversizeKey := make([]byte, 4097) // MaxKeySize is 4096
				batch.Put(oversizeKey, []byte("value"))
				return batch
			},
			expectError: true,
		},
		{
			name: "LargeValue",
			setupBatch: func() *txn.Batch {
				batch := txn.NewBatch()
				largeValue := make([]byte, 1024*1024) // 1MB value
				batch.Put([]byte("large_key"), largeValue)
				return batch
			},
			expectError: false,
		},
		{
			name: "EmptyValue",
			setupBatch: func() *txn.Batch {
				batch := txn.NewBatch()
				batch.Put([]byte("key"), []byte{})
				return batch
			},
			expectError: false,
		},
		{
			name: "BinaryData",
			setupBatch: func() *txn.Batch {
				batch := txn.NewBatch()
				binaryKey := []byte{0x00, 0x01, 0x02, 0xFF}
				binaryValue := []byte{0xFF, 0xFE, 0xFD, 0x00}
				batch.Put(binaryKey, binaryValue)
				return batch
			},
			expectError: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			dbPath := t.TempDir()
			setupTestDB(t, dbPath)

			db, err := waldb_db.Open(dbPath, logger.NoOpLogger{})
			tst.RequireNoError(t, err)
			defer func() {
				_ = db.Close()
			}()

			batch := tc.setupBatch()
			txnId, err := db.Commit(batch)

			if tc.expectError {
				tst.AssertNotNil(t, err, "expected error for %s", tc.name)
				tst.AssertEqual(t, txnId, 0, "expected zero txnId on error")
				tst.AssertTrue(t, errors.Is(err, waldb_db.ErrCommitInvalidBatch), "expected ErrCommitInvalidBatch")
			} else {
				tst.RequireNoError(t, err)
				tst.AssertGreaterThan(t, txnId, 0, "expected non-zero txnId")
			}
		})
	}
}

func TestCommitMultipleDeletes(t *testing.T) {
	dbPath := t.TempDir()
	setupTestDB(t, dbPath)

	db, err := waldb_db.Open(dbPath, logger.NoOpLogger{})
	tst.RequireNoError(t, err)
	defer func() {
		_ = db.Close()
	}()

	// First put some keys
	batch1 := txn.NewBatch()
	batch1.Put([]byte("key1"), []byte("value1"))
	batch1.Put([]byte("key2"), []byte("value2"))
	txnId1, err := db.Commit(batch1)
	tst.RequireNoError(t, err)
	tst.AssertGreaterThan(t, txnId1, 0, "expected non-zero txnId")

	// Then delete them
	batch2 := txn.NewBatch()
	batch2.Delete([]byte("key1"))
	batch2.Delete([]byte("key2"))
	txnId2, err := db.Commit(batch2)
	tst.RequireNoError(t, err)
	tst.AssertGreaterThan(t, txnId2, txnId1, "expected txnId2 > txnId1")
}

func TestCommitMixedOperations(t *testing.T) {
	dbPath := t.TempDir()
	setupTestDB(t, dbPath)

	db, err := waldb_db.Open(dbPath, logger.NoOpLogger{})
	tst.RequireNoError(t, err)
	defer func() {
		_ = db.Close()
	}()

	// First batch: put some keys
	batch1 := txn.NewBatch()
	batch1.Put([]byte("key1"), []byte("value1"))
	batch1.Put([]byte("key2"), []byte("value2"))
	batch1.Put([]byte("key3"), []byte("value3"))
	txnId1, err := db.Commit(batch1)
	tst.RequireNoError(t, err)
	tst.AssertGreaterThan(t, txnId1, 0, "expected non-zero txnId")

	// Second batch: mixed put and delete
	batch2 := txn.NewBatch()
	batch2.Put([]byte("key4"), []byte("value4"))
	batch2.Delete([]byte("key1"))
	batch2.Put([]byte("key2"), []byte("updated"))
	batch2.Delete([]byte("key3"))
	txnId2, err := db.Commit(batch2)
	tst.RequireNoError(t, err)
	tst.AssertGreaterThan(t, txnId2, txnId1, "expected txnId2 > txnId1")
}

func TestOpenCreatesDirectory(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := tmpDir + "/subdir"

	// Create the directory first
	err := os.MkdirAll(dbPath, 0o750)
	tst.RequireNoError(t, err)

	// Initialize manifest
	setupTestDB(t, dbPath)

	db, err := waldb_db.Open(dbPath, logger.NoOpLogger{})
	tst.RequireNoError(t, err)
	defer func() {
		_ = db.Close()
	}()

	// Verify directory was created
	_, err = os.Stat(dbPath)
	tst.RequireNoError(t, err)
}

// ============================================================================
// Acceptance Criteria Tests
// ============================================================================

// AC1: End-to-end DB flow
func TestEndToEndDBFlow_PutGet(t *testing.T) {
	dbPath := t.TempDir()
	setupTestDB(t, dbPath)

	// Open DB
	db, err := waldb_db.Open(dbPath, logger.NoOpLogger{})
	tst.RequireNoError(t, err)
	defer func() {
		_ = db.Close()
	}()

	// Put a key-value pair
	err = db.Put([]byte("testkey"), []byte("testvalue"))
	tst.RequireNoError(t, err)

	// Get and verify
	value, err := db.Get([]byte("testkey"))
	tst.RequireNoError(t, err)
	tst.AssertEqual(t, string(value), string([]byte("testvalue")), "expected retrieved value to match")
}

// AC1: End-to-end DB flow with Delete
func TestEndToEndDBFlow_DeleteGet(t *testing.T) {
	dbPath := t.TempDir()
	setupTestDB(t, dbPath)

	// Open DB
	db, err := waldb_db.Open(dbPath, logger.NoOpLogger{})
	tst.RequireNoError(t, err)
	defer func() {
		_ = db.Close()
	}()

	// Put a key-value pair
	err = db.Put([]byte("testkey"), []byte("testvalue"))
	tst.RequireNoError(t, err)

	// Delete the key
	err = db.Delete([]byte("testkey"))
	tst.RequireNoError(t, err)

	// Get and verify it returns ErrKeyNotFound
	_, err = db.Get([]byte("testkey"))
	tst.AssertTrue(t, errors.Is(err, waldb_db.ErrKeyNotFound), "expected ErrKeyNotFound after delete")
}

// AC1: End-to-end DB flow with Batch commit
func TestEndToEndDBFlow_BatchCommit(t *testing.T) {
	dbPath := t.TempDir()
	setupTestDB(t, dbPath)

	// Open DB
	db, err := waldb_db.Open(dbPath, logger.NoOpLogger{})
	tst.RequireNoError(t, err)
	defer func() {
		_ = db.Close()
	}()

	// Create a batch with multiple operations
	batch := txn.NewBatch()
	batch.Put([]byte("key1"), []byte("value1"))
	batch.Put([]byte("key2"), []byte("value2"))
	batch.Put([]byte("key3"), []byte("value3"))

	// Commit the batch
	txnId, err := db.Commit(batch)
	tst.RequireNoError(t, err)
	tst.AssertGreaterThan(t, txnId, 0, "expected non-zero txnId")

	// Verify all keys were committed
	v1, err := db.Get([]byte("key1"))
	tst.RequireNoError(t, err)
	tst.AssertEqual(t, string(v1), "value1", "expected key1 value")

	v2, err := db.Get([]byte("key2"))
	tst.RequireNoError(t, err)
	tst.AssertEqual(t, string(v2), "value2", "expected key2 value")

	v3, err := db.Get([]byte("key3"))
	tst.RequireNoError(t, err)
	tst.AssertEqual(t, string(v3), "value3", "expected key3 value")
}

// AC1: Restart DB and verify recovered state
func TestEndToEndDBFlow_RestartAndRecover(t *testing.T) {
	dbPath := t.TempDir()
	setupTestDB(t, dbPath)

	// First session: Open, Put, Close
	{
		db, err := waldb_db.Open(dbPath, logger.NoOpLogger{})
		tst.RequireNoError(t, err)

		err = db.Put([]byte("persistent_key"), []byte("persistent_value"))
		tst.RequireNoError(t, err)

		err = db.Close()
		tst.RequireNoError(t, err)
	}

	// Second session: Open, Get, verify
	{
		db, err := waldb_db.Open(dbPath, logger.NoOpLogger{})
		tst.RequireNoError(t, err)
		defer func() {
			_ = db.Close()
		}()

		value, err := db.Get([]byte("persistent_key"))
		tst.RequireNoError(t, err)
		tst.AssertEqual(t, string(value), "persistent_value", "expected recovered value after restart")
	}
}

// AC2: Single-writer enforcement - second concurrent open fails with ErrLocked
func TestSingleWriter_ConcurrentOpenFails(t *testing.T) {
	dbPath := t.TempDir()
	setupTestDB(t, dbPath)

	// First open succeeds
	db1, err := waldb_db.Open(dbPath, logger.NoOpLogger{})
	tst.RequireNoError(t, err)
	defer func() {
		_ = db1.Close()
	}()

	// Second open should fail with ErrLocked
	db2, err := waldb_db.Open(dbPath, logger.NoOpLogger{})
	tst.AssertNotNil(t, err, "expected error on second concurrent open")
	tst.AssertNil(t, db2, "expected nil DB on failed open")
	tst.AssertTrue(t, errors.Is(err, waldb_db.ErrLocked), "expected ErrLocked error")
}

// AC3: Limits - Empty key rejected
func TestLimits_EmptyKeyRejected(t *testing.T) {
	dbPath := t.TempDir()
	setupTestDB(t, dbPath)

	db, err := waldb_db.Open(dbPath, logger.NoOpLogger{})
	tst.RequireNoError(t, err)
	defer func() {
		_ = db.Close()
	}()

	// Try to put with empty key
	err = db.Put([]byte{}, []byte("value"))
	tst.AssertNotNil(t, err, "expected error for empty key")
	tst.AssertTrue(t, errors.Is(err, waldb_db.ErrInvalidKey), "expected ErrInvalidKey")

	// Try to delete with empty key
	err = db.Delete([]byte{})
	tst.AssertNotNil(t, err, "expected error for empty key on delete")
	tst.AssertTrue(t, errors.Is(err, waldb_db.ErrInvalidKey), "expected ErrInvalidKey")

	// Try to get with empty key
	_, err = db.Get([]byte{})
	tst.AssertNotNil(t, err, "expected error for empty key on get")
	tst.AssertTrue(t, errors.Is(err, waldb_db.ErrInvalidKey), "expected ErrInvalidKey")
}

// AC3: Limits - Oversize key rejected
func TestLimits_OversizeKeyRejected(t *testing.T) {
	dbPath := t.TempDir()
	setupTestDB(t, dbPath)

	db, err := waldb_db.Open(dbPath, logger.NoOpLogger{})
	tst.RequireNoError(t, err)
	defer func() {
		_ = db.Close()
	}()

	// Create a key larger than manifest limit (default is 4096)
	oversizeKey := make([]byte, 4097)

	// Try to put with oversize key
	err = db.Put(oversizeKey, []byte("value"))
	tst.AssertNotNil(t, err, "expected error for oversize key")
	tst.AssertTrue(t, errors.Is(err, waldb_db.ErrKeyTooLarge), "expected ErrKeyTooLarge")

	// Try to delete with oversize key
	err = db.Delete(oversizeKey)
	tst.AssertNotNil(t, err, "expected error for oversize key on delete")
	tst.AssertTrue(t, errors.Is(err, waldb_db.ErrKeyTooLarge), "expected ErrKeyTooLarge")

	// Try to get with oversize key
	_, err = db.Get(oversizeKey)
	tst.AssertNotNil(t, err, "expected error for oversize key on get")
	tst.AssertTrue(t, errors.Is(err, waldb_db.ErrKeyTooLarge), "expected ErrKeyTooLarge")
}

// AC3: Limits - Oversize value rejected
func TestLimits_OversizeValueRejected(t *testing.T) {
	dbPath := t.TempDir()
	setupTestDB(t, dbPath)

	db, err := waldb_db.Open(dbPath, logger.NoOpLogger{})
	tst.RequireNoError(t, err)
	defer func() {
		_ = db.Close()
	}()

	// Create a value larger than manifest limit
	// Use manifest max + 1 to avoid absurdly large allocations that are flaky in CI
	oversizeValue := make([]byte, record.MaxValueSize+1)

	// Try to put with oversize value
	err = db.Put([]byte("key"), oversizeValue)
	tst.AssertNotNil(t, err, "expected error for oversize value")
	tst.AssertTrue(t, errors.Is(err, waldb_db.ErrValueTooLarge), "expected ErrValueTooLarge")
}

// AC3: Rejected operations do not write to WAL
func TestLimits_RejectedOpsDoNotWriteToWAL(t *testing.T) {
	dbPath := t.TempDir()
	setupTestDB(t, dbPath)

	db, err := waldb_db.Open(dbPath, logger.NoOpLogger{})
	tst.RequireNoError(t, err)

	// Put a valid entry first
	err = db.Put([]byte("valid_key"), []byte("valid_value"))
	tst.RequireNoError(t, err)

	// Try to put with empty key (should be rejected)
	err = db.Put([]byte{}, []byte("value"))
	tst.AssertNotNil(t, err, "expected error for empty key")

	// Try to put with oversize key (should be rejected)
	oversizeKey := make([]byte, 4097)
	err = db.Put(oversizeKey, []byte("value"))
	tst.AssertNotNil(t, err, "expected error for oversize key")

	// Close and reopen to verify WAL was not written
	err = db.Close()
	tst.RequireNoError(t, err)

	db2, err := waldb_db.Open(dbPath, logger.NoOpLogger{})
	tst.RequireNoError(t, err)
	defer func() {
		_ = db2.Close()
	}()

	// Verify only the valid entry is in the database
	value, err := db2.Get([]byte("valid_key"))
	tst.RequireNoError(t, err)
	tst.AssertEqual(t, string(value), "valid_value", "expected valid entry to be recovered")

	// Verify rejected entries are not in the database
	_, err = db2.Get([]byte{})
	tst.AssertNotNil(t, err, "should not be able to get empty key")

	_, err = db2.Get(oversizeKey)
	tst.AssertNotNil(t, err, "should not be able to get oversize key")
}

// AC4: Concurrency - Concurrent Get calls succeed
func TestConcurrency_ConcurrentGetSucceeds(t *testing.T) {
	dbPath := t.TempDir()
	setupTestDB(t, dbPath)

	db, err := waldb_db.Open(dbPath, logger.NoOpLogger{})
	tst.RequireNoError(t, err)
	defer func() {
		_ = db.Close()
	}()

	// Put some data
	err = db.Put([]byte("concurrent_key"), []byte("concurrent_value"))
	tst.RequireNoError(t, err)

	// Launch multiple concurrent Get operations
	const goroutineCount = 10
	var wg sync.WaitGroup
	var successCount int32

	for i := 0; i < goroutineCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			value, err := db.Get([]byte("concurrent_key"))
			if err == nil && len(value) > 0 {
				atomic.AddInt32(&successCount, 1)
			}
		}()
	}

	wg.Wait()

	tst.AssertEqual(t, int(successCount), goroutineCount, "expected all concurrent Gets to succeed")
}

// AC4: Concurrency - Concurrent commits serialize correctly
func TestConcurrency_ConcurrentCommitsSerialize(t *testing.T) {
	dbPath := t.TempDir()
	setupTestDB(t, dbPath)

	db, err := waldb_db.Open(dbPath, logger.NoOpLogger{})
	tst.RequireNoError(t, err)
	defer func() {
		_ = db.Close()
	}()

	// Launch multiple concurrent commits
	const goroutineCount = 5
	var wg sync.WaitGroup
	var txnIds []uint64
	var mu sync.Mutex

	for i := 0; i < goroutineCount; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			batch := txn.NewBatch()
			key := []byte("concurrent_key_" + string(rune(index)))
			batch.Put(key, []byte("value"))
			txnId, err := db.Commit(batch)
			if err == nil {
				mu.Lock()
				txnIds = append(txnIds, txnId)
				mu.Unlock()
			}
		}(i)
	}

	wg.Wait()

	// Verify all commits succeeded
	tst.AssertEqual(t, len(txnIds), goroutineCount, "expected all concurrent commits to succeed")

	// Sort txnIds to verify they form a monotonically increasing sequence
	sort.Slice(txnIds, func(i, j int) bool { return txnIds[i] < txnIds[j] })
	// Verify txn IDs are monotonically increasing (after sorting)
	for i := 1; i < len(txnIds); i++ {
		tst.AssertTrue(t, txnIds[i] > txnIds[i-1], "expected txn IDs to be monotonically increasing")
	}
}

// AC4: No corruption under contention
func TestConcurrency_NoCorruptionUnderContention(t *testing.T) {
	dbPath := t.TempDir()
	setupTestDB(t, dbPath)

	db, err := waldb_db.Open(dbPath, logger.NoOpLogger{})
	tst.RequireNoError(t, err)
	defer func() {
		_ = db.Close()
	}()

	// Launch mixed concurrent operations
	const opCount = 20
	var wg sync.WaitGroup

	for i := 0; i < opCount; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			key := []byte("key_" + string(rune(index%5)))
			value := []byte("value_" + string(rune(index)))

			// Alternate between Put and Get
			if index%2 == 0 {
				_ = db.Put(key, value)
			} else {
				_, _ = db.Get(key)
			}
		}(i)
	}

	wg.Wait()

	// Verify database is still usable
	err = db.Put([]byte("final_test"), []byte("final_value"))
	tst.RequireNoError(t, err)

	value, err := db.Get([]byte("final_test"))
	tst.RequireNoError(t, err)
	tst.AssertEqual(t, string(value), "final_value", "expected database to be uncorrupted after concurrent operations")
}

// ============================================================================
// Gap Tests - Close() Racing with Operations
// ============================================================================

// TestCloseRacingWithGet verifies behavior when Close() is called while Get is in-flight
func TestCloseRacingWithGet(t *testing.T) {
	dbPath := t.TempDir()
	setupTestDB(t, dbPath)

	db, err := waldb_db.Open(dbPath, logger.NoOpLogger{})
	tst.RequireNoError(t, err)

	// Put a value
	err = db.Put([]byte("key"), []byte("value"))
	tst.RequireNoError(t, err)

	// Launch a Get in a separate goroutine
	var getErr error
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_, getErr = db.Get([]byte("key"))
	}()

	// Immediately close the DB
	closeErr := db.Close()
	tst.RequireNoError(t, closeErr)

	// Wait for Get to complete
	wg.Wait()

	// Get should either succeed (if it ran before close) or fail with ErrClosed
	if getErr != nil {
		tst.AssertTrue(t, errors.Is(getErr, waldb_db.ErrClosed), "expected ErrClosed if Get ran after Close")
	}
}

// TestCloseRacingWithPut verifies behavior when Close() is called while Put is in-flight
func TestCloseRacingWithPut(t *testing.T) {
	dbPath := t.TempDir()
	setupTestDB(t, dbPath)

	db, err := waldb_db.Open(dbPath, logger.NoOpLogger{})
	tst.RequireNoError(t, err)

	// Launch a Put in a separate goroutine
	var putErr error
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		putErr = db.Put([]byte("key"), []byte("value"))
	}()

	// Immediately close the DB
	closeErr := db.Close()
	tst.RequireNoError(t, closeErr)

	// Wait for Put to complete
	wg.Wait()

	// Put should either succeed (if it ran before close) or fail with ErrClosed
	if putErr != nil {
		tst.AssertTrue(t, errors.Is(putErr, waldb_db.ErrClosed), "expected ErrClosed if Put ran after Close")
	}
}

// TestCloseRacingWithCommit verifies behavior when Close() is called while Commit is in-flight
func TestCloseRacingWithCommit(t *testing.T) {
	dbPath := t.TempDir()
	setupTestDB(t, dbPath)

	db, err := waldb_db.Open(dbPath, logger.NoOpLogger{})
	tst.RequireNoError(t, err)

	// Launch a Commit in a separate goroutine
	var commitErr error
	var txnId uint64
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		batch := txn.NewBatch()
		batch.Put([]byte("key"), []byte("value"))
		txnId, commitErr = db.Commit(batch)
	}()

	// Immediately close the DB
	closeErr := db.Close()
	tst.RequireNoError(t, closeErr)

	// Wait for Commit to complete
	wg.Wait()

	// Commit should either succeed (if it ran before close) or fail with ErrClosed
	if commitErr != nil {
		tst.AssertTrue(t, errors.Is(commitErr, waldb_db.ErrClosed), "expected ErrClosed if Commit ran after Close")
	} else {
		tst.AssertGreaterThan(t, txnId, uint64(0), "expected non-zero txnId if Commit succeeded")
	}
}

// TestOperationsFailAfterClose verifies that all operations fail after Close
func TestOperationsFailAfterClose(t *testing.T) {
	dbPath := t.TempDir()
	setupTestDB(t, dbPath)

	db, err := waldb_db.Open(dbPath, logger.NoOpLogger{})
	tst.RequireNoError(t, err)

	// Close the database
	err = db.Close()
	tst.RequireNoError(t, err)

	// All subsequent operations should fail with ErrClosed
	_, err = db.Get([]byte("key"))
	tst.AssertTrue(t, errors.Is(err, waldb_db.ErrClosed), "expected Get to fail after Close")

	err = db.Put([]byte("key"), []byte("value"))
	tst.AssertTrue(t, errors.Is(err, waldb_db.ErrClosed), "expected Put to fail after Close")

	err = db.Delete([]byte("key"))
	tst.AssertTrue(t, errors.Is(err, waldb_db.ErrClosed), "expected Delete to fail after Close")

	batch := txn.NewBatch()
	batch.Put([]byte("key"), []byte("value"))
	_, err = db.Commit(batch)
	tst.AssertTrue(t, errors.Is(err, waldb_db.ErrClosed), "expected Commit to fail after Close")
}
