package db

import (
	"errors"
	"io/fs"

	"github.com/julianstephens/waldb/internal/logger"
	"github.com/julianstephens/waldb/internal/waldb/manifest"
	"github.com/julianstephens/waldb/internal/waldb/memtable"
	"github.com/julianstephens/waldb/internal/waldb/recovery"
	"github.com/julianstephens/waldb/internal/waldb/txn"
	wl "github.com/julianstephens/waldb/internal/waldb/wal"
)

// DB represents a WAL-based database instance.
type DB struct {
	dir      string
	wal      *wl.Log
	txnw     *txn.Writer
	memtable *memtable.Table
	manifest *manifest.Manifest
	logger   logger.Logger
	closed   bool
}

// Open opens an existing WAL database at the given directory path.
// The directory and its manifest must already exist; otherwise an error is returned.
// An optional logger can be provided; if nil, a no-op logger is used.
// Returns the opened DB instance or an error.
func Open(dir string, lg logger.Logger) (*DB, error) {
	if dir == "" {
		return nil, wrapDBErr("open", ErrInvalidDir, dir, nil)
	}

	if lg == nil {
		lg = &logger.NoOpLogger{}
	}

	mf, err := manifest.Open(dir)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			lg.Error("manifest not found", err, "dir", dir)
			return nil, wrapDBErr("open", ErrManifestNotFound, dir, err)
		}
		lg.Error("failed to load manifest", err, "dir", dir)
		return nil, wrapDBErr("open", ErrManifestLoadFailed, dir, err)
	}

	lg.Info("opening database", "dir", dir, "fsync_on_commit", mf.FsyncOnCommit)

	db := &DB{
		dir:      dir,
		logger:   lg,
		closed:   false,
		manifest: mf,
	}

	if err := db.initialize(); err != nil {
		lg.Error("failed to initialize database", err, "dir", dir)
		return nil, err
	}

	lg.Info("database opened successfully", "dir", dir)
	return db, nil
}

// Close closes the database.
func (db *DB) Close() error {
	if db.closed {
		return wrapDBErr("close", ErrClosed, db.dir, nil)
	}

	db.logger.Info("closing database", "dir", db.dir)

	if err := db.wal.Close(); err != nil {
		db.logger.Error("failed to close WAL log", err, "dir", db.dir)
		return wrapDBErr("close", ErrCloseFailed, db.dir, err)
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
	return db.closed
}

// Commit applies the operations in the given batch as a single transaction.
// It writes the transaction to the WAL and updates the in-memory state.
func (db *DB) Commit(b *txn.Batch) (uint64, error) {
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

// Get retrieves the value associated with the given key from the in-memory memtable.
// Returns the value and true if the key exists and is not deleted, otherwise returns nil and false.
func (db *DB) Get(key []byte) ([]byte, bool) {
	value, ok := db.memtable.Get(key)
	db.logger.Debug("get operation", "key_size", len(key), "found", ok, "value_size", len(value))
	return value, ok
}

// initialize sets up the WAL log, memtable, and replays existing transactions to restore the database state.
func (db *DB) initialize() error {
	log, err := wl.OpenLog(db.dir, wl.LogOpts{SegmentMaxBytes: int64(db.manifest.WalSegmentMaxBytes)}, db.logger)
	if err != nil {
		db.logger.Error("failed to open WAL log", err, "dir", db.dir)
		return wrapDBErr("open", ErrWALOpenFailed, db.dir, err)
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
