package e2e_test

import (
	"errors"
	"fmt"
	"os"
	"testing"

	tst "github.com/julianstephens/go-utils/tests"
	"github.com/julianstephens/waldb/internal/logger"
	"github.com/julianstephens/waldb/internal/waldb"
	waldb_db "github.com/julianstephens/waldb/internal/waldb/db"
	_ "github.com/julianstephens/waldb/internal/waldb/manifest" // register manifest initializer
	"github.com/julianstephens/waldb/internal/waldb/txn"
)

// ============================================================================
// Init End-to-End Tests
// ============================================================================

// E2E: Init → Open → Put → Close → Reopen → Get
// Verifies that data written after Init survives a full restart.
func TestInit_FullLifecycle_DataPersists(t *testing.T) {
	dbPath := t.TempDir() + "/e2e-persist"

	err := waldb.Init(dbPath, logger.NoOpLogger{})
	tst.RequireNoError(t, err)

	// First session: write data.
	{
		db, err := waldb_db.Open(dbPath, logger.NoOpLogger{})
		tst.RequireNoError(t, err)

		err = db.Put([]byte("e2e_key"), []byte("e2e_value"))
		tst.RequireNoError(t, err)

		err = db.Close()
		tst.RequireNoError(t, err)
	}

	// Second session: verify data survives restart.
	{
		db, err := waldb_db.Open(dbPath, logger.NoOpLogger{})
		tst.RequireNoError(t, err)
		defer func() { _ = db.Close() }()

		val, err := db.Get([]byte("e2e_key"))
		tst.RequireNoError(t, err)
		tst.AssertEqual(t, "e2e_value", string(val), "expected value to persist across restart")
	}
}

// E2E: Init → Open → Batch commit → Close → Reopen → Get all keys
// Verifies that a committed batch is fully durable.
func TestInit_BatchCommit_AllKeysPersist(t *testing.T) {
	dbPath := t.TempDir() + "/e2e-batch"

	err := waldb.Init(dbPath, logger.NoOpLogger{})
	tst.RequireNoError(t, err)

	keys := [][]byte{[]byte("alpha"), []byte("beta"), []byte("gamma")}
	vals := [][]byte{[]byte("1"), []byte("2"), []byte("3")}

	// First session: commit a batch.
	{
		db, err := waldb_db.Open(dbPath, logger.NoOpLogger{})
		tst.RequireNoError(t, err)

		batch := txn.NewBatch()
		for i, k := range keys {
			batch.Put(k, vals[i])
		}

		txnId, err := db.Commit(batch)
		tst.RequireNoError(t, err)
		tst.AssertGreaterThan(t, txnId, uint64(0), "expected non-zero txnId")

		err = db.Close()
		tst.RequireNoError(t, err)
	}

	// Second session: verify all keys are present.
	{
		db, err := waldb_db.Open(dbPath, logger.NoOpLogger{})
		tst.RequireNoError(t, err)
		defer func() { _ = db.Close() }()

		for i, k := range keys {
			val, err := db.Get(k)
			tst.RequireNoError(t, err)
			tst.AssertEqual(t, string(vals[i]), string(val), "expected batch key %q to persist", string(k))
		}
	}
}

// E2E: Init on a non-empty dir fails; pre-existing DB is still accessible.
func TestInit_DoubleInit_ExistingDBUnaffected(t *testing.T) {
	dbPath := t.TempDir() + "/e2e-double"

	err := waldb.Init(dbPath, logger.NoOpLogger{})
	tst.RequireNoError(t, err)

	// Write data via first session.
	{
		db, err := waldb_db.Open(dbPath, logger.NoOpLogger{})
		tst.RequireNoError(t, err)
		err = db.Put([]byte("guarded"), []byte("intact"))
		tst.RequireNoError(t, err)
		err = db.Close()
		tst.RequireNoError(t, err)
	}

	// Second Init call must be rejected.
	err = waldb.Init(dbPath, logger.NoOpLogger{})
	tst.AssertNotNil(t, err, "expected error when re-initializing a non-empty directory")
	tst.AssertTrue(t, errors.Is(err, waldb.ErrAlreadyExists), "expected ErrAlreadyExists")

	// The existing DB must remain intact.
	{
		db, err := waldb_db.Open(dbPath, logger.NoOpLogger{})
		tst.RequireNoError(t, err)
		defer func() { _ = db.Close() }()

		val, err := db.Get([]byte("guarded"))
		tst.RequireNoError(t, err)
		tst.AssertEqual(t, "intact", string(val), "expected DB data to be intact after failed re-init")
	}
}

// E2E: Init → multiple write sessions accumulate data correctly.
func TestInit_MultipleSessions_DataAccumulates(t *testing.T) {
	dbPath := t.TempDir() + "/e2e-sessions"

	err := waldb.Init(dbPath, logger.NoOpLogger{})
	tst.RequireNoError(t, err)

	const sessionCount = 3
	for i := 0; i < sessionCount; i++ {
		db, err := waldb_db.Open(dbPath, logger.NoOpLogger{})
		tst.RequireNoError(t, err)

		key := []byte(fmt.Sprintf("session_key_%d", i))
		val := []byte(fmt.Sprintf("session_val_%d", i))
		err = db.Put(key, val)
		tst.RequireNoError(t, err)

		err = db.Close()
		tst.RequireNoError(t, err)
	}

	// Final session: verify all keys from all sessions are present.
	db, err := waldb_db.Open(dbPath, logger.NoOpLogger{})
	tst.RequireNoError(t, err)
	defer func() { _ = db.Close() }()

	for i := 0; i < sessionCount; i++ {
		key := []byte(fmt.Sprintf("session_key_%d", i))
		val := []byte(fmt.Sprintf("session_val_%d", i))
		got, err := db.Get(key)
		tst.RequireNoError(t, err)
		tst.AssertEqual(t, string(val), string(got), fmt.Sprintf("expected key from session %d to persist", i))
	}
}

// E2E: Init on path that is a regular file returns ErrInvalidDir; no layout is created.
func TestInit_FilePathRejected_NoSideEffects(t *testing.T) {
	// Create a regular file where the DB directory would be.
	f, err := os.CreateTemp(t.TempDir(), "not-a-dir-*")
	tst.RequireNoError(t, err)
	filePath := f.Name()
	tst.RequireNoError(t, f.Close())

	err = waldb.Init(filePath, logger.NoOpLogger{})
	tst.AssertNotNil(t, err, "expected error when db path is a file")
	tst.AssertTrue(t, errors.Is(err, waldb.ErrInvalidDir), "expected ErrInvalidDir")

	// The file itself must not have been replaced or removed.
	info, statErr := os.Stat(filePath)
	tst.RequireNoError(t, statErr)
	tst.AssertTrue(t, info.Mode().IsRegular(), "expected original file to remain untouched")
}

// E2E: Init → Open → Delete a key → Close → Reopen → key is absent.
func TestInit_DeletePersistsAcrossRestart(t *testing.T) {
	dbPath := t.TempDir() + "/e2e-delete"

	err := waldb.Init(dbPath, logger.NoOpLogger{})
	tst.RequireNoError(t, err)

	// Write then delete.
	{
		db, err := waldb_db.Open(dbPath, logger.NoOpLogger{})
		tst.RequireNoError(t, err)

		err = db.Put([]byte("gone"), []byte("soon"))
		tst.RequireNoError(t, err)

		err = db.Delete([]byte("gone"))
		tst.RequireNoError(t, err)

		err = db.Close()
		tst.RequireNoError(t, err)
	}

	// Verify deletion is durable.
	{
		db, err := waldb_db.Open(dbPath, logger.NoOpLogger{})
		tst.RequireNoError(t, err)
		defer func() { _ = db.Close() }()

		_, err = db.Get([]byte("gone"))
		tst.AssertNotNil(t, err, "expected key to be absent after delete and restart")
		tst.AssertTrue(t, errors.Is(err, waldb_db.ErrKeyNotFound), "expected ErrKeyNotFound")
	}
}
