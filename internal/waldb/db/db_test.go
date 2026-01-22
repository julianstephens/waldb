package db_test

import (
	"errors"
	"os"
	"testing"

	tst "github.com/julianstephens/go-utils/tests"
	waldb "github.com/julianstephens/waldb/internal/waldb/db"
	"github.com/julianstephens/waldb/internal/waldb/txn"
)

func TestOpenClose(t *testing.T) {
	db, err := waldb.Open(t.TempDir() + "/test.db")
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
	tst.AssertNotNil(t, err, "expected error on double close")
}

func TestOpenEmptyPath(t *testing.T) {
	_, err := waldb.Open("")
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
			db, err := waldb.Open(t.TempDir() + "/test.db")
			tst.RequireNoError(t, err)
			defer func() {
				_ = db.Close()
			}()

			batch := tc.setupBatch()
			txnId, err := db.Commit(batch)

			if tc.expectError {
				tst.AssertNotNil(t, err, "expected error for %s", tc.name)
				tst.AssertEqual(t, txnId, 0, "expected zero txnId on error")
				tst.AssertTrue(t, errors.Is(err, waldb.ErrCommitInvalidBatch), "expected ErrCommitInvalidBatch")
			} else {
				tst.RequireNoError(t, err)
				tst.AssertGreaterThan(t, txnId, 0, "expected non-zero txnId")
			}
		})
	}
}

func TestCommitMultipleDeletes(t *testing.T) {
	db, err := waldb.Open(t.TempDir() + "/test.db")
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
	db, err := waldb.Open(t.TempDir() + "/test.db")
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
	dbPath := tmpDir + "/subdir/test.db"

	db, err := waldb.Open(dbPath)
	tst.RequireNoError(t, err)
	defer func() {
		_ = db.Close()
	}()

	// Verify directory was created
	_, err = os.Stat(dbPath)
	tst.RequireNoError(t, err)
}
