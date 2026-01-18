package e2e_test

import (
	"path/filepath"
	"testing"

	tst "github.com/julianstephens/go-utils/tests"
	waldb "github.com/julianstephens/waldb/internal/waldb/db"
	"github.com/julianstephens/waldb/internal/waldb/txn"
)

func TestIdAcrossRestart(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test.db")

	// 1. Open(path)
	db, err := waldb.Open(dbPath)
	tst.RequireNoError(t, err)

	// 2. Commit batch A
	batchA := txn.NewBatch()
	batchA.Put([]byte("key-a"), []byte("val-a"))
	txnIdA, err := db.Commit(batchA)
	tst.RequireNoError(t, err)
	tst.AssertGreaterThan(t, txnIdA, uint64(0), "first txnId should be > 0")

	// 3. Close
	err = db.Close()
	tst.RequireNoError(t, err)

	// 4. Open(path) again
	db2, err := waldb.Open(dbPath)
	tst.RequireNoError(t, err)
	defer func() {
		_ = db2.Close()
	}()

	// 5. Commit batch B
	batchB := txn.NewBatch()
	batchB.Put([]byte("key-b"), []byte("val-b"))
	txnIdB, err := db2.Commit(batchB)
	tst.RequireNoError(t, err)

	// 6. Assert txn IDs are strictly increasing
	tst.AssertGreaterThan(t, txnIdB, txnIdA, "txnId after restart should be greater than previous txnId")
}
