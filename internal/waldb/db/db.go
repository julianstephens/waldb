package db

import (
	"errors"
	"fmt"
	"io/fs"
	"path/filepath"
	"sync"

	"github.com/gofrs/flock"

	"github.com/julianstephens/go-utils/helpers"
	"github.com/julianstephens/waldb/internal/logger"
	"github.com/julianstephens/waldb/internal/waldb"
	"github.com/julianstephens/waldb/internal/waldb/manifest"
	"github.com/julianstephens/waldb/internal/waldb/memtable"
	"github.com/julianstephens/waldb/internal/waldb/recovery"
	"github.com/julianstephens/waldb/internal/waldb/txn"
	wl "github.com/julianstephens/waldb/internal/waldb/wal"
)

// DB represents a WAL-based database instance.
type DB struct {
	dir      string
	lock     *flock.Flock
	wal      *wl.Log
	txnw     *txn.Writer
	memtable *memtable.Table
	manifest *manifest.Manifest
	mu       *sync.RWMutex
	logger   logger.Logger
	closed   bool
}

// Open opens an existing WAL database at the given directory path.
// The directory and its manifest must already exist; otherwise an error is returned.
// An optional logger can be provided; if nil, a no-op logger is used.
// Returns the opened DB instance or an error.
func Open(dir string, lg logger.Logger) (*DB, error) {
	if err := validateDBDir(dir); err != nil {
		return nil, err
	}

	if lg == nil {
		lg = &logger.NoOpLogger{}
	}

	mf, err := manifest.Open(dir)
	if err != nil {
		if errors.Is(err, manifest.ErrManifestNotFound) {
			lg.Error("manifest not found", err, "dir", dir)
			return nil, wrapDBErr("open", ErrManifestMissing, dir, err)
		}
		if errors.Is(err, manifest.ErrManifestUnsupportedVersion) {
			lg.Error("manifest has unsupported version", err, "dir", dir)
			return nil, wrapDBErr("open", ErrFormatNotSupported, dir, err)
		}
		lg.Error("failed to open manifest", err, "dir", dir)
		return nil, wrapDBErr("open", ErrManifestInvalid, dir, err)
	}

	lg.Info("opening database", "dir", dir, "fsync_on_commit", mf.FsyncOnCommit)

	db := &DB{
		dir:      dir,
		logger:   lg,
		closed:   false,
		manifest: mf,
		mu:       &sync.RWMutex{},
	}

	if err := db.acquireLock(); err != nil {
		return nil, err
	}

	if err := db.initialize(); err != nil {
		lg.Error("failed to initialize database", err, "dir", dir)
	} else {
		lg.Info("database opened successfully", "dir", dir)
	}
	return db, db.cleanupOnError(err)
}

// Close closes the database. If the database is already closed, it does nothing and returns nil.
func (db *DB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.closed {
		return nil
	}

	db.logger.Info("closing database", "dir", db.dir)

	if err := db.wal.Close(); err != nil {
		db.logger.Error("failed to close WAL log", err, "dir", db.dir)
		return wrapDBErr("close", ErrCloseFailed, db.dir, err)
	}

	if err := db.releaseLock(); err != nil {
		return err
	}

	db.closed = true
	return nil
}

// Path returns the database path.
func (db *DB) Path() string {
	return db.dir
}

// IsClosed returns true if the database is closed.
func (db *DB) IsClosed() bool {
	db.mu.RLock()
	defer db.mu.RUnlock()

	return db.closed
}

// Commit applies the operations in the given batch as a single transaction.
// It writes the transaction to the WAL and updates the in-memory state.
// Returns the transaction ID or an error.
func (db *DB) Commit(b *txn.Batch) (uint64, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.closed {
		return 0, wrapDBErr("commit", ErrClosed, db.dir, nil)
	}

	return db._commit(b)
}

func (db *DB) _commit(b *txn.Batch) (uint64, error) {
	txnId, err := db.txnw.Commit(b)
	if err != nil {
		db.logger.Error("commit failed", err, "dir", db.dir, "count", len(b.Ops()))
		if errors.Is(err, txn.ErrCommitInvalidBatch) {
			return 0, wrapDBErr("commit", ErrCommitInvalidBatch, db.dir, err)
		}
		return 0, wrapDBErr("commit", ErrCommitFailed, db.dir, err)
	}

	if err := db.memtable.Apply(b.Ops()); err != nil {
		db.logger.Error("failed to apply batch to memtable", err, "dir", db.dir, "txn", txnId)
		return txnId, wrapDBErr("commit", ErrCommitFailed, db.dir, err)
	}

	db.logger.Info("commit successful", "txn", txnId, "count", len(b.Ops()))
	return txnId, nil
}

// Get retrieves the value associated with the given key.
// If the key does not exist, it returns "not found".
func (db *DB) Get(key []byte) ([]byte, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	if db.closed {
		return nil, wrapDBErr("get", ErrClosed, db.dir, nil)
	}

	if err := db.validateKey(key); err != nil {
		db.logger.Error("invalid key for get operation", err, "key_size", len(key))
		if dbErr, ok := err.(*DBError); ok {
			dbErr.Op = "get"
			return nil, dbErr
		}
		return nil, err
	}
	value, ok := db.memtable.Get(key)
	db.logger.Debug("get operation", "key_size", len(key), "found", ok, "value_size", len(value))
	if !ok {
		return nil, wrapDBErr("get", ErrKeyNotFound, db.dir, nil)
	}
	return value, nil
}

// Put sets the value for the given key in the database.
// It validates the key and value sizes before committing the operation.
func (db *DB) Put(key, value []byte) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.closed {
		return wrapDBErr("put", ErrClosed, db.dir, nil)
	}

	if err := db.validateKey(key); err != nil {
		db.logger.Error("invalid key for put operation", err, "key_size", len(key))
		if dbErr, ok := err.(*DBError); ok {
			dbErr.Op = "put"
			return dbErr
		}
		return err
	}
	if err := db.validateValue(value); err != nil {
		db.logger.Error("invalid value for put operation", err, "value_size", len(value))
		if dbErr, ok := err.(*DBError); ok {
			dbErr.Op = "put"
			return dbErr
		}
		return err
	}
	b := txn.NewBatch()
	b.Put(key, value)
	if _, err := db._commit(b); err != nil {
		return err
	}

	db.logger.Debug("put operation", "key_size", len(key), "value_size", len(value))
	return nil
}

func (db *DB) Delete(key []byte) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.closed {
		return wrapDBErr("delete", ErrClosed, db.dir, nil)
	}

	if err := db.validateKey(key); err != nil {
		db.logger.Error("invalid key for delete operation", err, "key_size", len(key))
		if dbErr, ok := err.(*DBError); ok {
			dbErr.Op = "delete"
			return dbErr
		}
		return err
	}

	b := txn.NewBatch()
	b.Delete(key)
	if _, err := db._commit(b); err != nil {
		return err
	}

	db.logger.Debug("delete operation", "key_size", len(key))
	return nil
}

// initialize sets up the WAL log, memtable, and replays existing transactions to restore the database state.
func (db *DB) initialize() error {
	logDir := filepath.Join(db.dir, waldb.WALDirName)
	log, err := wl.OpenLog(logDir, wl.LogOpts{SegmentMaxBytes: int64(db.manifest.WalSegmentMaxBytes)}, db.logger)
	if err != nil {
		db.logger.Error("failed to open WAL log", err, "dir", logDir)
		return wrapDBErr("open", ErrWALOpenFailed, logDir, err)
	}
	db.wal = log
	db.memtable = memtable.New()

	segIds := db.wal.SegmentIDs()
	start := wl.Boundary{SegId: wl.FirstSegmentID, Offset: 0}
	if len(segIds) > 0 {
		start.SegId = segIds[0]
	}

	db.logger.Info("starting recovery", "seg_count", len(segIds))
	res, err := recovery.Replay(db.wal, start, db.memtable, db.logger)
	if err != nil {
		db.logger.Error("recovery failed", err, "dir", db.dir)
		return wrapDBErr("replay", ErrReplayFailed, db.dir, err)
	}

	db.logger.Info("recovery complete", "next_txn_id", res.NextTxnId, "last_committed_txn_id", res.LastCommittedTxnId)

	allocator, err := txn.NewCounterAllocator(res.NextTxnId)
	if err != nil {
		db.logger.Error("failed to create transaction allocator", err, "dir", db.dir, "next_txn_id", res.NextTxnId)
		return wrapDBErr("init", ErrInitFailed, db.dir, err)
	}

	db.txnw = txn.NewWriter(allocator, db.wal, txn.WriterOpts{FsyncOnCommit: db.manifest.FsyncOnCommit}, db.logger)

	return nil
}

// validateDBDir checks if the given directory is a valid WAL database directory
// by verifying the presence and validity of required files and subdirectories.
func validateDBDir(dir string) error {
	if dir == "" {
		return wrapDBErr("open", ErrInvalidDir, dir, errors.New("directory path is empty"))
	}
	if _, err := validatePath(dir, true); err != nil {
		return wrapDBErr("open", ErrInvalidDir, dir, err)
	}

	manifestPath := filepath.Join(dir, waldb.ManifestFileName)
	info, err := validatePath(manifestPath, false)
	if err != nil {
		return wrapDBErr("open", ErrManifestMissing, dir, err)
	}
	if !info.Mode().IsRegular() {
		return wrapDBErr(
			"open",
			ErrManifestInvalid,
			dir,
			fmt.Errorf("manifest is not a regular file: %s", manifestPath),
		)
	}

	lockPath := filepath.Join(dir, waldb.LockFileName)
	info, err = validatePath(lockPath, false)
	if err != nil {
		return wrapDBErr("open", ErrInvalidDir, dir, fmt.Errorf("lock file invalid: %w", err))
	}
	if !info.Mode().IsRegular() {
		return wrapDBErr("open", ErrInvalidDir, dir, fmt.Errorf("lock file is not a regular file: %s", lockPath))
	}

	walDir := filepath.Join(dir, waldb.WALDirName)
	if _, err := validatePath(walDir, true); err != nil {
		return wrapDBErr("open", ErrInvalidDir, dir, fmt.Errorf("WAL directory missing: %w", err))
	}

	return nil
}

func validatePath(path string, isDir bool) (info fs.FileInfo, err error) {
	exists, info, err := helpers.ExistsWithInfo(path)
	if err != nil {
		return
	}
	if !exists {
		err = fmt.Errorf("path does not exist: %s", path)
		return
	}
	if isDir && !info.IsDir() {
		err = fmt.Errorf("expected directory but found file: %s", path)
		return
	}
	if !isDir && info.IsDir() {
		err = fmt.Errorf("expected file but found directory: %s", path)
		return
	}
	return
}

// acquireLock attempts to acquire a file lock on the database directory to prevent concurrent access.
func (db *DB) acquireLock() error {
	lockPath := filepath.Join(db.dir, waldb.LockFileName)
	fileLock := flock.New(lockPath)

	locked, err := fileLock.TryLock()
	if err != nil {
		db.logger.Error("failed to acquire file lock", err, "lock_path", lockPath)
		return wrapDBErr("open", ErrOpenFailed, db.dir, err)
	}
	if !locked {
		db.logger.Error("database is already locked by another process", nil, "lock_path", lockPath)
		return wrapDBErr("open", ErrLocked, db.dir, errors.New("database is locked"))
	}

	db.lock = fileLock

	db.logger.Info("acquired database lock", "lock_path", lockPath)
	return nil
}

// releaseLock releases the file lock on the database directory.
func (db *DB) releaseLock() error {
	if db.lock == nil {
		return nil
	}

	if err := db.lock.Unlock(); err != nil {
		db.logger.Error("failed to release database lock", err, "dir", db.dir)
		return wrapDBErr("close", ErrCloseFailed, db.dir, err)
	}

	db.logger.Info("released database lock", "dir", db.dir)
	return nil
}

func (db *DB) cleanupOnError(err error) error {
	if err == nil {
		return nil
	}

	lg := db.logger
	if db.wal != nil {
		if err := db.wal.Close(); err != nil {
			lg.Error("failed to close WAL during cleanup", err, "dir", db.dir)
		}
	}

	if db.lock != nil {
		if err := db.releaseLock(); err != nil {
			lg.Error("failed to release lock during cleanup", err, "dir", db.dir)
		}
	}
	return err
}

func (db *DB) validateKey(key []byte) error {
	if key == nil {
		return &DBError{
			Err:   ErrInvalidKey,
			Cause: errors.New("key is nil"),
			Path:  db.dir,
		}
	}
	if len(key) == 0 {
		return &DBError{
			Err:   ErrInvalidKey,
			Cause: errors.New("key is empty"),
			Path:  db.dir,
		}
	}
	if len(key) > db.manifest.MaxKeyBytes {
		return &DBError{
			Err:   ErrKeyTooLarge,
			Cause: errors.New("key is size exceeds maximum"),
			Path:  db.dir,
		}
	}
	return nil
}

func (db *DB) validateValue(value []byte) error {
	if value == nil {
		return &DBError{
			Err:   ErrInvalidValue,
			Cause: errors.New("value is nil"),
			Path:  db.dir,
		}
	}
	if len(value) > db.manifest.MaxValueBytes {
		return &DBError{
			Err:   ErrValueTooLarge,
			Cause: errors.New("value size exceeds maximum"),
			Path:  db.dir,
		}
	}
	return nil
}
