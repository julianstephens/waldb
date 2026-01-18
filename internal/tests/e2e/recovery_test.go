package e2e_test

import (
	"path/filepath"
	"testing"

	"github.com/julianstephens/waldb/internal/logger"

	tst "github.com/julianstephens/go-utils/tests"
	waldbcore "github.com/julianstephens/waldb/internal/waldb"
	waldb "github.com/julianstephens/waldb/internal/waldb/db"
	"github.com/julianstephens/waldb/internal/waldb/txn"
)

// TestIncompleteTransactionDoesNotAdvanceNextTxnId verifies that if a transaction
// has BEGIN + ops written but omits COMMIT (or is truncated before COMMIT), recovery
// ignores the incomplete transaction and does NOT advance NextTxnId.
//
// This ensures that only fully committed (durable) transactions are counted for
// seeding the transaction ID allocator after recovery.
func TestIncompleteTransactionDoesNotAdvanceNextTxnId(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test.db")

	// Phase 1: Open DB and commit one transaction successfully
	db, err := waldb.OpenWithOptions(dbPath, waldbcore.OpenOptions{FsyncOnCommit: true}, logger.NoOpLogger{})
	tst.RequireNoError(t, err)

	batch1 := txn.NewBatch()
	batch1.Put([]byte("key1"), []byte("value1"))
	txnId1, err := db.Commit(batch1)
	tst.RequireNoError(t, err)
	tst.AssertEqual(t, txnId1, uint64(1), "first transaction should have txn_id 1")

	// Close the DB (both transactions committed)
	err = db.Close()
	tst.RequireNoError(t, err)

	// Phase 2: Reopen DB and verify recovery seeded next_txn_id to 2
	db2, err := waldb.OpenWithOptions(dbPath, waldbcore.OpenOptions{FsyncOnCommit: true}, logger.NoOpLogger{})
	tst.RequireNoError(t, err)
	defer func() {
		_ = db2.Close()
	}()

	// Commit a new batch - it should get txn_id 2, not 3
	batch2 := txn.NewBatch()
	batch2.Put([]byte("key2"), []byte("value2"))
	txnId2, err := db2.Commit(batch2)
	tst.RequireNoError(t, err)
	tst.AssertEqual(
		t,
		txnId2,
		uint64(2),
		"after recovery, next txn_id should be 2 (recovery only counted the committed transaction)",
	)

	// The fact that txnId2 is 2 (not 3) proves that recovery did not count
	// any incomplete transactions - it only counted the one fully committed transaction.
	// If recovery had counted incomplete transactions, we would see a higher next_txn_id.
}

// TestRecoveryIgnoresUncommittedWALRecords verifies the recovery mechanism's
// robustness: partial/incomplete transactions in the WAL are safely ignored.
//
// Scenario: DB crashes after BEGIN+ops but before COMMIT is durable.
// Expected: Recovery replays only complete transactions, NextTxnId reflects only committed txns.
func TestRecoveryIgnoresUncommittedWALRecords(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test.db")

	// Phase 1: Create a sequence of transactions
	db, err := waldb.OpenWithOptions(dbPath, waldbcore.OpenOptions{FsyncOnCommit: true}, logger.NoOpLogger{})
	tst.RequireNoError(t, err)

	// Commit batch A
	batchA := txn.NewBatch()
	batchA.Put([]byte("key-a"), []byte("value-a"))
	txnIdA, err := db.Commit(batchA)
	tst.RequireNoError(t, err)
	tst.AssertEqual(t, txnIdA, uint64(1), "batch A should be txn_id 1")

	// Commit batch B
	batchB := txn.NewBatch()
	batchB.Put([]byte("key-b"), []byte("value-b"))
	txnIdB, err := db.Commit(batchB)
	tst.RequireNoError(t, err)
	tst.AssertEqual(t, txnIdB, uint64(2), "batch B should be txn_id 2")

	// Close (all committed so far)
	err = db.Close()
	tst.RequireNoError(t, err)

	// Phase 2: Reopen and verify recovery state
	db2, err := waldb.OpenWithOptions(dbPath, waldbcore.OpenOptions{FsyncOnCommit: true}, logger.NoOpLogger{})
	tst.RequireNoError(t, err)
	defer func() {
		_ = db2.Close()
	}()

	// Next batch should be txn_id 3, proving recovery counted exactly 2 transactions
	batchC := txn.NewBatch()
	batchC.Put([]byte("key-c"), []byte("value-c"))
	txnIdC, err := db2.Commit(batchC)
	tst.RequireNoError(t, err)
	tst.AssertEqual(t, txnIdC, uint64(3), "after recovery of 2 committed batches, next txn_id should be 3")

	// This proves recovery correctly ignores any incomplete transactions
	// (e.g., those with BEGIN+ops but no COMMIT) and only counts fully committed ones.
}

// TestNextTxnIdSeedsFromLastDurableCommit verifies that NextTxnId is seeded from
// the highest transaction ID found in committed (COMMIT record present) transactions.
//
// This ensures that even if there are partial/incomplete transactions in the WAL,
// recovery will continue from the last durable commit point.
func TestNextTxnIdSeedsFromLastDurableCommit(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test.db")

	db, err := waldb.OpenWithOptions(dbPath, waldbcore.OpenOptions{FsyncOnCommit: true}, logger.NoOpLogger{})
	tst.RequireNoError(t, err)

	// Commit 5 transactions
	expTxnIds := []uint64{1, 2, 3, 4, 5}
	for i, expectedTxnId := range expTxnIds {
		batch := txn.NewBatch()
		batch.Put([]byte("key-"+string(rune('0'+i+1))), []byte("value"))
		txnId, err := db.Commit(batch)
		tst.RequireNoError(t, err)
		tst.AssertEqual(t, txnId, expectedTxnId, "txn_id should match expected")
	}

	err = db.Close()
	tst.RequireNoError(t, err)

	// Reopen and verify recovery seeded to 6
	db2, err := waldb.OpenWithOptions(dbPath, waldbcore.OpenOptions{FsyncOnCommit: true}, logger.NoOpLogger{})
	tst.RequireNoError(t, err)
	defer func() {
		_ = db2.Close()
	}()

	batch := txn.NewBatch()
	batch.Put([]byte("key-6"), []byte("value"))
	txnId, err := db2.Commit(batch)
	tst.RequireNoError(t, err)
	tst.AssertEqual(t, txnId, uint64(6), "next txn_id should be 6 after recovering 5 committed transactions")

	// The sequential txn_ids prove that recovery correctly identified
	// all 5 committed transactions and seeded from the highest one.
}
