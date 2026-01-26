package e2e_test

import (
	"os"
	"path/filepath"
	"testing"

	tst "github.com/julianstephens/go-utils/tests"
	"github.com/julianstephens/waldb/internal/logger"
	waldb "github.com/julianstephens/waldb/internal/waldb/db"
	"github.com/julianstephens/waldb/internal/waldb/manifest"
	"github.com/julianstephens/waldb/internal/waldb/txn"
)

// TestIdStartsAtOne verifies that a brand-new DB starts with txn_id at 1.
// This tests that the allocator is initialized correctly with no recovery data.
func TestIdStartsAtOne(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test.db")

	tst.RequireNoError(t, os.MkdirAll(dbPath, 0o750))
	_, err := manifest.Init(dbPath)
	tst.RequireNoError(t, err)

	// Open a brand-new DB (no segments)
	db, err := waldb.Open(dbPath, logger.NoOpLogger{})
	tst.RequireNoError(t, err)
	defer func() {
		_ = db.Close()
	}()

	// Commit a batch
	batch := txn.NewBatch()
	batch.Put([]byte("key"), []byte("value"))
	txnId, err := db.Commit(batch)
	tst.RequireNoError(t, err)

	// Assert that the first txn_id is exactly 1
	tst.AssertEqual(t, txnId, uint64(1), "first txn_id should be 1 in a brand-new DB")
}

// TestIdSeededFromRecovery verifies that the allocator is seeded from recovery output.
// This tests that when a DB is reopened, the txn_id allocator is properly initialized
// with the NextTxnId value from recovery of committed transactions.
func TestIdSeededFromRecovery(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test.db")

	tst.RequireNoError(t, os.MkdirAll(dbPath, 0o750))
	_, err := manifest.Init(dbPath)
	tst.RequireNoError(t, err)

	// Phase 1: Create a DB and commit multiple batches
	db, err := waldb.Open(dbPath, logger.NoOpLogger{})
	tst.RequireNoError(t, err)

	var txnIds []uint64
	for i := 0; i < 3; i++ {
		batch := txn.NewBatch()
		batch.Put([]byte("key-"+string(rune('a'+i))), []byte("value"))
		txnId, err := db.Commit(batch)
		tst.RequireNoError(t, err)
		txnIds = append(txnIds, txnId)
	}

	// Verify transactions are monotonically increasing
	tst.AssertEqual(t, txnIds[0], uint64(1), "first txn_id should be 1")
	tst.AssertEqual(t, txnIds[1], uint64(2), "second txn_id should be 2")
	tst.AssertEqual(t, txnIds[2], uint64(3), "third txn_id should be 3")

	lastTxnIdBeforeClose := txnIds[2]

	// Close the DB
	err = db.Close()
	tst.RequireNoError(t, err)

	// Phase 2: Reopen the DB and verify allocator is seeded from recovery
	db2, err := waldb.Open(dbPath, logger.NoOpLogger{})
	tst.RequireNoError(t, err)
	defer func() {
		_ = db2.Close()
	}()

	// Commit a new batch
	batch := txn.NewBatch()
	batch.Put([]byte("key-d"), []byte("value"))
	txnIdAfterReopen, err := db2.Commit(batch)
	tst.RequireNoError(t, err)

	// Assert that the allocator continued from where it left off
	expectedNextTxnId := lastTxnIdBeforeClose + 1
	tst.AssertEqual(t, txnIdAfterReopen, expectedNextTxnId, "txn_id should continue from recovery point")
}

func TestIdAcrossRestart(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test.db")

	tst.RequireNoError(t, os.MkdirAll(dbPath, 0o750))
	_, err := manifest.Init(dbPath)
	tst.RequireNoError(t, err)

	// 1. Open(path) - fresh DB
	db, err := waldb.Open(dbPath, logger.NoOpLogger{})
	tst.RequireNoError(t, err)

	// 2. Commit batch A
	batchA := txn.NewBatch()
	batchA.Put([]byte("key-a"), []byte("val-a"))
	txnIdA, err := db.Commit(batchA)
	tst.RequireNoError(t, err)
	tst.AssertEqual(t, txnIdA, uint64(1), "first txnId in fresh DB should be exactly 1")

	// 3. Close
	err = db.Close()
	tst.RequireNoError(t, err)

	// 4. Open(path) again
	db2, err := waldb.Open(dbPath, logger.NoOpLogger{})
	tst.RequireNoError(t, err)
	defer func() {
		_ = db2.Close()
	}()

	// 5. Commit batch B
	batchB := txn.NewBatch()
	batchB.Put([]byte("key-b"), []byte("val-b"))
	txnIdB, err := db2.Commit(batchB)
	tst.RequireNoError(t, err)

	// 6. Assert txn IDs are consecutive with no gaps (txnIdB == txnIdA + 1)
	expectedTxnIdB := txnIdA + 1
	tst.AssertEqual(t, txnIdB, expectedTxnIdB, "txnId after restart should be exactly txnIdA+1 (no gaps allowed)")
}
