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

func TestCommitValidBatch(t *testing.T) {
	db, err := waldb.Open(t.TempDir() + "/test.db")
	tst.RequireNoError(t, err)
	defer func() {
		_ = db.Close()
	}()

	batch := txn.NewBatch()
	batch.Put([]byte("key1"), []byte("value1"))

	err = db.Commit(batch)
	tst.RequireNoError(t, err)
}

func TestCommitMultiplePuts(t *testing.T) {
	db, err := waldb.Open(t.TempDir() + "/test.db")
	tst.RequireNoError(t, err)
	defer func() {
		_ = db.Close()
	}()

	batch := txn.NewBatch()
	batch.Put([]byte("key1"), []byte("value1"))
	batch.Put([]byte("key2"), []byte("value2"))
	batch.Put([]byte("key3"), []byte("value3"))

	err = db.Commit(batch)
	tst.RequireNoError(t, err)
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
	err = db.Commit(batch1)
	tst.RequireNoError(t, err)

	// Then delete them
	batch2 := txn.NewBatch()
	batch2.Delete([]byte("key1"))
	batch2.Delete([]byte("key2"))
	err = db.Commit(batch2)
	tst.RequireNoError(t, err)
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
	err = db.Commit(batch1)
	tst.RequireNoError(t, err)

	// Second batch: mixed put and delete
	batch2 := txn.NewBatch()
	batch2.Put([]byte("key4"), []byte("value4"))
	batch2.Delete([]byte("key1"))
	batch2.Put([]byte("key2"), []byte("updated"))
	batch2.Delete([]byte("key3"))
	err = db.Commit(batch2)
	tst.RequireNoError(t, err)
}

func TestCommitEmptyBatch(t *testing.T) {
	db, err := waldb.Open(t.TempDir() + "/test.db")
	tst.RequireNoError(t, err)
	defer func() {
		_ = db.Close()
	}()

	batch := txn.NewBatch()
	err = db.Commit(batch)
	tst.AssertNotNil(t, err, "expected error for empty batch")

	// Verify it's a commit invalid batch error
	var dbErr *waldb.DBError
	tst.AssertTrue(t, errors.As(err, &dbErr), "expected DBError")
	tst.AssertTrue(t, errors.Is(err, waldb.ErrCommitInvalidBatch), "expected ErrCommitInvalidBatch")
}

func TestCommitInvalidKey(t *testing.T) {
	db, err := waldb.Open(t.TempDir() + "/test.db")
	tst.RequireNoError(t, err)
	defer func() {
		_ = db.Close()
	}()

	batch := txn.NewBatch()
	batch.Put(nil, []byte("value"))

	err = db.Commit(batch)
	tst.AssertNotNil(t, err, "expected error for nil key")
	tst.AssertTrue(t, errors.Is(err, waldb.ErrCommitInvalidBatch), "expected ErrCommitInvalidBatch")
}

func TestCommitOversizeKey(t *testing.T) {
	db, err := waldb.Open(t.TempDir() + "/test.db")
	tst.RequireNoError(t, err)
	defer func() {
		_ = db.Close()
	}()

	batch := txn.NewBatch()
	oversizeKey := make([]byte, 4097) // MaxKeySize is 4096
	batch.Put(oversizeKey, []byte("value"))

	err = db.Commit(batch)
	tst.AssertNotNil(t, err, "expected error for oversized key")
	tst.AssertTrue(t, errors.Is(err, waldb.ErrCommitInvalidBatch), "expected ErrCommitInvalidBatch")
}

func TestMultipleSequentialCommits(t *testing.T) {
	db, err := waldb.Open(t.TempDir() + "/test.db")
	tst.RequireNoError(t, err)
	defer func() {
		_ = db.Close()
	}()

	// Commit 1
	batch1 := txn.NewBatch()
	batch1.Put([]byte("key1"), []byte("value1"))
	err = db.Commit(batch1)
	tst.RequireNoError(t, err)

	// Commit 2
	batch2 := txn.NewBatch()
	batch2.Put([]byte("key2"), []byte("value2"))
	err = db.Commit(batch2)
	tst.RequireNoError(t, err)

	// Commit 3
	batch3 := txn.NewBatch()
	batch3.Put([]byte("key3"), []byte("value3"))
	err = db.Commit(batch3)
	tst.RequireNoError(t, err)
}

func TestCommitLargeValue(t *testing.T) {
	db, err := waldb.Open(t.TempDir() + "/test.db")
	tst.RequireNoError(t, err)
	defer func() {
		_ = db.Close()
	}()

	batch := txn.NewBatch()
	largeValue := make([]byte, 1024*1024) // 1MB value
	batch.Put([]byte("large_key"), largeValue)

	err = db.Commit(batch)
	tst.RequireNoError(t, err)
}

func TestCommitEmptyValue(t *testing.T) {
	db, err := waldb.Open(t.TempDir() + "/test.db")
	tst.RequireNoError(t, err)
	defer func() {
		_ = db.Close()
	}()

	batch := txn.NewBatch()
	batch.Put([]byte("key"), []byte{}) // Empty value

	err = db.Commit(batch)
	tst.RequireNoError(t, err)
}

func TestCommitBinaryData(t *testing.T) {
	db, err := waldb.Open(t.TempDir() + "/test.db")
	tst.RequireNoError(t, err)
	defer func() {
		_ = db.Close()
	}()

	batch := txn.NewBatch()
	binaryKey := []byte{0x00, 0x01, 0x02, 0xFF}
	binaryValue := []byte{0xFF, 0xFE, 0xFD, 0x00}
	batch.Put(binaryKey, binaryValue)

	err = db.Commit(batch)
	tst.RequireNoError(t, err)
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
