package db

import (
	"errors"

	"github.com/julianstephens/waldb/internal/logger"
	"github.com/julianstephens/waldb/internal/waldb"
	"github.com/julianstephens/waldb/internal/waldb/memtable"
	"github.com/julianstephens/waldb/internal/waldb/recovery"
	"github.com/julianstephens/waldb/internal/waldb/txn"
	wl "github.com/julianstephens/waldb/internal/waldb/wal"
)

// DB represents a WAL-based database instance.
type DB struct {
	path     string
	wal      *wl.Log
	txnw     *txn.Writer
	memtable *memtable.Table
	opts     waldb.OpenOptions
	logger   logger.Logger
	closed   bool
}

// Open opens or creates a WAL database at the given path with no logging.
func Open(path string) (*DB, error) {
	return OpenWithOptions(path, waldb.OpenOptions{}, logger.NoOpLogger{})
}

// OpenWithOptions opens or creates a WAL database with the given options and logger.
// The caller is responsible for managing the logger lifecycle (including closing).
// If logger is nil, a NoOpLogger is used.
func OpenWithOptions(path string, opts waldb.OpenOptions, lg logger.Logger) (*DB, error) {
	if path == "" {
		return nil, wrapDBErr("open", ErrInvalidPath, path, nil)
	}

	if lg == nil {
		lg = logger.NoOpLogger{}
	}

	lg.Info("opening database", "path", path, "fsync_on_commit", opts.FsyncOnCommit)

	db := &DB{
		path:   path,
		logger: lg,
		closed: false,
		opts:   opts,
	}

	if err := db.initialize(); err != nil {
		lg.Error("failed to initialize database", err, "path", path)
		return nil, err
	}

	lg.Info("database opened successfully", "path", path)
	return db, nil
}

// Close closes the database.
func (db *DB) Close() error {
	if db.closed {
		return wrapDBErr("close", ErrClosed, db.path, nil)
	}

	db.logger.Info("closing database", "path", db.path)

	if err := db.wal.Close(); err != nil {
		db.logger.Error("failed to close WAL log", err, "path", db.path)
		return wrapDBErr("close", ErrCloseFailed, db.path, err)
	}

	db.closed = true
	return nil
}

// Path returns the database path.
func (db *DB) Path() string {
	return db.path
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
		db.logger.Error("commit failed", err, "path", db.path, "count", len(b.Ops()))
		if errors.Is(err, txn.ErrCommitInvalidBatch) {
			return 0, wrapDBErr("commit", ErrCommitInvalidBatch, db.path, err)
		}
		return 0, wrapDBErr("commit", ErrCommitFailed, db.path, err)
	}

	if err := db.memtable.Apply(b.Ops()); err != nil {
		db.logger.Error("failed to apply batch to memtable", err, "path", db.path, "txn", txnId)
		return txnId, wrapDBErr("commit", ErrCommitFailed, db.path, err)
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

func (db *DB) initialize() error {
	log, err := wl.OpenLog(db.path, wl.LogOpts{SegmentMaxBytes: waldb.DefaultSegmentMaxBytes}, db.logger)
	if err != nil {
		db.logger.Error("failed to open WAL log", err, "path", db.path)
		return wrapDBErr("open", ErrWALOpenFailed, db.path, err)
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
		db.logger.Error("recovery failed", err, "path", db.path)
		return wrapDBErr("replay", ErrReplayFailed, db.path, err)
	}

	db.logger.Info("recovery complete", "next_txn_id", res.NextTxnId, "last_committed_txn_id", res.LastCommittedTxnId)

	allocator, err := txn.NewCounterAllocator(res.NextTxnId)
	if err != nil {
		db.logger.Error("failed to create transaction allocator", err, "path", db.path, "next_txn_id", res.NextTxnId)
		return wrapDBErr("init", ErrInitFailed, db.path, err)
	}

	db.txnw = txn.NewWriter(allocator, db.wal, txn.WriterOpts{FsyncOnCommit: db.opts.FsyncOnCommit}, db.logger)

	return nil
}
