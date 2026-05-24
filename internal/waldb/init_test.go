package waldb_test

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	tst "github.com/julianstephens/go-utils/tests"

	"github.com/julianstephens/waldb/internal/logger"
	"github.com/julianstephens/waldb/internal/waldb"
	waldb_db "github.com/julianstephens/waldb/internal/waldb/db"
	"github.com/julianstephens/waldb/internal/waldb/manifest"
	_ "github.com/julianstephens/waldb/internal/waldb/manifest" // register manifest initializer
)

// ============================================================================
// Init Tests
// ============================================================================

// AC1: Init on a fresh directory followed by Open succeeds.
func TestInit_FreshDir_ThenOpen(t *testing.T) {
	dbPath := t.TempDir() + "/freshdb"

	err := waldb.Init(dbPath, logger.NoOpLogger{})
	tst.RequireNoError(t, err)

	db, err := waldb_db.Open(dbPath, logger.NoOpLogger{})
	tst.RequireNoError(t, err)
	defer func() { _ = db.Close() }()

	tst.AssertFalse(t, db.IsClosed(), "expected database to be open after Init+Open")
}

// AC1: Init returns ErrInvalidDir when the given path exists but is a file, not a directory.
func TestInit_PathIsFile_ReturnsErrInvalidDir(t *testing.T) {
	// Create a regular file at the target path.
	f, err := os.CreateTemp(t.TempDir(), "not-a-dir-*")
	tst.RequireNoError(t, err)
	filePath := f.Name()
	tst.RequireNoError(t, f.Close())

	err = waldb.Init(filePath, logger.NoOpLogger{})
	tst.AssertNotNil(t, err, "expected error when db path is a file")
	tst.AssertTrue(t, errors.Is(err, waldb.ErrInvalidDir), "expected ErrInvalidDir when path is a file")
}

// AC1: Init on a fresh directory creates the expected layout.
func TestInit_FreshDir_CreatesLayout(t *testing.T) {
	dbPath := t.TempDir() + "/layoutdb"

	err := waldb.Init(dbPath, logger.NoOpLogger{})
	tst.RequireNoError(t, err)

	// WAL directory
	walDir := dbPath + "/" + waldb.WALDirName
	info, statErr := os.Stat(walDir)
	tst.RequireNoError(t, statErr)
	tst.AssertTrue(t, info.IsDir(), "expected WAL directory to exist")

	// Lock file
	lockPath := dbPath + "/" + waldb.LockFileName
	info, statErr = os.Stat(lockPath)
	tst.RequireNoError(t, statErr)
	tst.AssertTrue(t, info.Mode().IsRegular(), "expected lock file to be a regular file")

	// Manifest
	manifestPath := dbPath + "/" + manifest.ManifestFileName
	info, statErr = os.Stat(manifestPath)
	tst.RequireNoError(t, statErr)
	tst.AssertTrue(t, info.Mode().IsRegular(), "expected manifest to be a regular file")
}

// AC1: Init with nil logger uses no-op logger without panicking.
func TestInit_NilLogger(t *testing.T) {
	dbPath := t.TempDir() + "/nilloggerdb"

	err := waldb.Init(dbPath, nil)
	tst.RequireNoError(t, err)
}

// AC1: A DB initialized via Init can perform Put and Get after Open.
func TestInit_ThenPutAndGet(t *testing.T) {
	dbPath := t.TempDir() + "/putgetdb"

	err := waldb.Init(dbPath, logger.NoOpLogger{})
	tst.RequireNoError(t, err)

	db, err := waldb_db.Open(dbPath, logger.NoOpLogger{})
	tst.RequireNoError(t, err)
	defer func() { _ = db.Close() }()

	err = db.Put([]byte("hello"), []byte("world"))
	tst.RequireNoError(t, err)

	val, err := db.Get([]byte("hello"))
	tst.RequireNoError(t, err)
	tst.AssertEqual(t, "world", string(val), "expected Get to return the value written by Put")
}

// AC2: Re-initializing an already-initialized DB returns ErrAlreadyExists.
func TestInit_AlreadyInitialized_ReturnsError(t *testing.T) {
	dbPath := t.TempDir() + "/reinitdb"

	// First init must succeed.
	err := waldb.Init(dbPath, logger.NoOpLogger{})
	tst.RequireNoError(t, err)

	// Second init on the same (non-empty) directory must fail with ErrAlreadyExists.
	err = waldb.Init(dbPath, logger.NoOpLogger{})
	tst.AssertNotNil(t, err, "expected error when re-initializing an existing DB")
	tst.AssertTrue(t, errors.Is(err, waldb.ErrAlreadyExists),
		"expected ErrAlreadyExists when re-initializing an existing DB")
}

// AC2: Re-initializing does not overwrite or corrupt the existing DB.
func TestInit_AlreadyInitialized_DoesNotCorrupt(t *testing.T) {
	dbPath := t.TempDir() + "/nocorruptdb"

	err := waldb.Init(dbPath, logger.NoOpLogger{})
	tst.RequireNoError(t, err)

	// Write data to the initialized DB.
	db, err := waldb_db.Open(dbPath, logger.NoOpLogger{})
	tst.RequireNoError(t, err)
	err = db.Put([]byte("safe"), []byte("data"))
	tst.RequireNoError(t, err)
	err = db.Close()
	tst.RequireNoError(t, err)

	// Re-init must fail (non-empty dir).
	_ = waldb.Init(dbPath, logger.NoOpLogger{})

	// Re-open and verify data is still present.
	db2, err := waldb_db.Open(dbPath, logger.NoOpLogger{})
	tst.RequireNoError(t, err)
	defer func() { _ = db2.Close() }()

	val, err := db2.Get([]byte("safe"))
	tst.RequireNoError(t, err)
	tst.AssertEqual(t, "data", string(val), "expected data to survive failed re-init")
}

// AC3: A directory that contains only a conflicting MANIFEST.json entry (as a
// subdirectory) causes Init to fail before creating any additional layout files.
// The pre-existing state is left unchanged (no WAL dir or LOCK created).
func TestInit_ConflictingManifestDir_EarlyRejection(t *testing.T) {
	dbPath := t.TempDir() + "/conflictdb"

	// Pre-create the DB directory with a MANIFEST.json subdirectory to simulate
	// a conflicting path before Init runs.
	manifestConflict := dbPath + "/" + manifest.ManifestFileName
	err := os.MkdirAll(manifestConflict, 0o750)
	tst.RequireNoError(t, err)

	// Init must fail because the directory is non-empty.
	err = waldb.Init(dbPath, logger.NoOpLogger{})
	tst.AssertNotNil(t, err, "expected Init to fail when manifest path is a directory")
	tst.AssertTrue(t, errors.Is(err, waldb.ErrAlreadyExists), "expected ErrAlreadyExists")

	// The WAL directory must NOT have been created (Init bailed out early).
	walDir := dbPath + "/" + waldb.WALDirName
	_, statErr := os.Stat(walDir)
	tst.AssertTrue(t, os.IsNotExist(statErr),
		"expected WAL directory to be absent after early Init failure")
}

// AC3: When manifest.Init fails, Init cleans up the WAL directory and lock file
// it created so no partial state is left on disk.
func TestInit_ManifestFailure_CleansUpCreatedFiles(t *testing.T) {
	dbPath := t.TempDir() + "/cleanupdb"

	// Inject a failing manifest init function.
	manifestErr := errors.New("simulated manifest failure")
	*waldb.ManifestInitFn = func(dir string) error {
		return manifestErr
	}
	t.Cleanup(waldb.ResetManifestInitFn)

	err := waldb.Init(dbPath, logger.NoOpLogger{})
	tst.AssertNotNil(t, err, "expected Init to fail when manifest.Init fails")
	tst.AssertTrue(t, errors.Is(err, waldb.ErrInitFailed), "expected ErrInitFailed")

	// WAL directory must have been removed by cleanup.
	_, statErr := os.Stat(filepath.Join(dbPath, waldb.WALDirName))
	tst.AssertTrue(t, os.IsNotExist(statErr),
		"expected WAL directory to be cleaned up after manifest failure")

	// Lock file must have been removed by cleanup.
	_, statErr = os.Stat(filepath.Join(dbPath, waldb.LockFileName))
	tst.AssertTrue(t, os.IsNotExist(statErr),
		"expected lock file to be cleaned up after manifest failure")
}
