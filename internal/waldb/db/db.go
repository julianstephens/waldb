package db

import (
	"errors"

	"github.com/julianstephens/waldb/internal/waldb"
	"github.com/julianstephens/waldb/internal/waldb/memtable"
	"github.com/julianstephens/waldb/internal/waldb/recovery"
	"github.com/julianstephens/waldb/internal/waldb/txn"
	"github.com/julianstephens/waldb/internal/waldb/wal"
)

// DB represents a WAL-based database instance.
type DB struct {
	path     string
	log      *wal.Log
	txnw     *txn.Writer
	memtable *memtable.Table
	opts     waldb.OpenOptions
	closed   bool
}

// Open opens or creates a WAL database at the given path.
// This is a placeholder implementation.
func Open(path string) (*DB, error) {
	return OpenWithOptions(path, waldb.OpenOptions{})
}

func OpenWithOptions(path string, opts waldb.OpenOptions) (*DB, error) {
	if path == "" {
		return nil, wrapDBErr("open", ErrInvalidPath, path, nil)
	}

	db := &DB{
		path:   path,
		closed: false,
		opts:   opts,
	}

	if err := db.initialize(); err != nil {
		return nil, err
	}

	return db, nil
}

// Close closes the database.
func (db *DB) Close() error {
	if db.closed {
		return wrapDBErr("close", ErrClosed, db.path, nil)
	}
	if err := db.log.Close(); err != nil {
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
		if errors.Is(err, txn.ErrCommitInvalidBatch) {
			return 0, wrapDBErr("commit", ErrCommitInvalidBatch, db.path, err)
		}
		return 0, wrapDBErr("commit", ErrCommitFailed, db.path, err)
	}

	if err := db.memtable.Apply(b.Ops()); err != nil {
		return txnId, wrapDBErr("commit", ErrCommitFailed, db.path, err)
	}

	return txnId, nil
}

func (db *DB) initialize() error {
	log, err := wal.OpenLog(db.path, wal.LogOpts{SegmentMaxBytes: waldb.DefaultSegmentMaxBytes})
	if err != nil {
		return wrapDBErr("open", ErrWALOpenFailed, db.path, err)
	}
	db.log = log
	db.memtable = memtable.New()

	segIds := db.log.SegmentIDs()
	start := wal.Boundary{SegId: wal.FirstSegmentID, Offset: 0}
	if len(segIds) > 0 {
		start.SegId = segIds[0]
	}

	res, err := recovery.Replay(db.log, start, db.memtable)
	if err != nil {
		return wrapDBErr("replay", ErrReplayFailed, db.path, err)
	}

	allocator, err := txn.NewCounterAllocator(res.NextTxnId)
	if err != nil {
		return wrapDBErr("init", ErrInitFailed, db.path, err)
	}

	db.txnw = txn.NewWriter(allocator, db.log, txn.WriterOpts{FsyncOnCommit: db.opts.FsyncOnCommit})

	return nil
}
