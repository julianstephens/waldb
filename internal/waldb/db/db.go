package db

import (
	"errors"

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
	closed   bool
}

// Open opens or creates a WAL database at the given path.
// This is a placeholder implementation.
func Open(path string) (*DB, error) {
	if path == "" {
		return nil, wrapDBErr("open", ErrInvalidPath, path, nil)
	}

	db := &DB{
		path:   path,
		closed: false,
	}

	if err := db.initialize(); err != nil {
		return nil, err
	}

	return db, nil
}

// Close closes the database.
// This is a placeholder implementation.
func (db *DB) Close() error {
	if db.closed {
		return wrapDBErr("close", ErrClosed, db.path, nil)
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
func (db *DB) Commit(b *txn.Batch) error {
	if _, err := db.txnw.Commit(b); err != nil {
		if errors.Is(err, txn.ErrCommitInvalidBatch) {
			return wrapDBErr("commit", ErrCommitInvalidBatch, db.path, err)
		}
		return wrapDBErr("commit", ErrCommitFailed, db.path, err)
	}

	ops := b.Ops()
	memOps := make([]memtable.Op, len(ops))
	for i, op := range ops {
		var memOpKind memtable.OpKind
		switch op.Kind {
		case txn.OpPut:
			memOpKind = memtable.OpPut
		case txn.OpDelete:
			memOpKind = memtable.OpDelete
		default:
			return wrapDBErr("commit", ErrCommitFailed, db.path, errors.New("unknown operation kind"))
		}
		memOps[i] = memtable.Op{
			Kind:  memOpKind,
			Key:   op.Key,
			Value: op.Value,
		}
	}

	if err := db.memtable.Apply(memOps); err != nil {
		return wrapDBErr("commit", ErrCommitFailed, db.path, err)
	}

	return nil
}

func (db *DB) initialize() error {
	log, err := wal.OpenLog(db.path, wal.LogOpts{})
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

	db.txnw = txn.NewWriter(allocator, db.log, txn.WriterOpts{})

	return nil
}
