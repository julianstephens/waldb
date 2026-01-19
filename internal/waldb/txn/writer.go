package txn

import (
	"github.com/julianstephens/waldb/internal/logger"
	"github.com/julianstephens/waldb/internal/waldb/kv"
	"github.com/julianstephens/waldb/internal/waldb/wal"
	"github.com/julianstephens/waldb/internal/waldb/wal/record"
)

type WriterOpts struct {
	FsyncOnCommit bool
}

type Writer struct {
	idAllocator IDAllocator
	logAppender wal.LogAppender
	logger      logger.Logger
	opts        WriterOpts
}

// NewWriter creates a new Writer that writes transactions to the given LogAppender.
func NewWriter(allocator IDAllocator, logAppender wal.LogAppender, opts WriterOpts, lg logger.Logger) *Writer {
	if lg == nil {
		lg = logger.NoOpLogger{}
	}
	return &Writer{
		idAllocator: allocator,
		logAppender: logAppender,
		logger:      lg,
		opts:        opts,
	}
}

// Commit writes a transaction consisting of the operations in the given batch to the WAL.
// It returns the assigned transaction ID upon success.
func (w *Writer) Commit(batch *Batch) (txnId uint64, err error) {
	if err2 := batch.Validate(); err2 != nil {
		w.logger.Warn("batch validation failed", "count", len(batch.Ops()), "reason", "invalid_batch")
		err = &CommitError{
			Err:   ErrCommitInvalidBatch,
			Stage: StageValidateBatch,
			Cause: err2,
		}
		return
	}

	txnId = w.idAllocator.Next()
	w.logger.Debug("allocated txn id", "txn", txnId, "ops_count", len(batch.Ops()))

	beginTxn, err2 := record.EncodeBeginTxnPayload(txnId)
	if err2 != nil {
		w.logger.Error("failed to encode begin txn record", err2, "txn", txnId)
		err = wrapCommitErr(StageEncodeBegin, ErrCommitEncodeBegin, txnId, err2)
		return
	}
	_, err2 = w.logAppender.Append(record.RecordTypeBeginTransaction, beginTxn)
	if err2 != nil {
		w.logger.Error("failed to append begin txn record", err2, "txn", txnId)
		err = wrapCommitErr(StageAppendBegin, ErrCommitAppendBegin, txnId, err2)
		return
	}

	opCount := 0
	for i, op := range batch.Ops() {
		switch op.Kind {
		case kv.OpPut:
			opCount++
			w.logger.Debug("processing put operation", "txn", txnId, "op_index", i, "key_size", len(op.Key))
			putOp, err2 := record.EncodePutOpPayload(txnId, op.Key, op.Value)
			if err2 != nil {
				w.logger.Error(
					"failed to encode put operation",
					err2,
					"txn",
					txnId,
					"op_index",
					i,
					"key_size",
					len(op.Key),
				)
				err = wrapCommitOpErr(StageEncodeOp, ErrCommitEncodeOp, txnId, i, op, err2)
				return
			}
			_, err2 = w.logAppender.Append(record.RecordTypePutOperation, putOp)
			if err2 != nil {
				w.logger.Error("failed to append put operation", err2, "txn", txnId, "op_index", i)
				err = wrapCommitOpErr(StageAppendOp, ErrCommitAppendOp, txnId, i, op, err2)
				return
			}
		case kv.OpDelete:
			opCount++
			w.logger.Debug("processing delete operation", "txn", txnId, "op_index", i, "key_size", len(op.Key))
			deleteOp, err2 := record.EncodeDeleteOpPayload(txnId, op.Key)
			if err2 != nil {
				w.logger.Error(
					"failed to encode delete operation",
					err2,
					"txn",
					txnId,
					"op_index",
					i,
					"key_size",
					len(op.Key),
				)
				err = wrapCommitOpErr(StageEncodeOp, ErrCommitEncodeOp, txnId, i, op, err2)
				return
			}
			_, err2 = w.logAppender.Append(record.RecordTypeDeleteOperation, deleteOp)
			if err2 != nil {
				w.logger.Error("failed to append delete operation", err2, "txn", txnId, "op_index", i)
				err = wrapCommitOpErr(StageAppendOp, ErrCommitAppendOp, txnId, i, op, err2)
				return
			}
		}
	}

	commitTxn, err2 := record.EncodeCommitTxnPayload(txnId)
	if err2 != nil {
		w.logger.Error("failed to encode commit txn record", err2, "txn", txnId)
		err = wrapCommitErr(StageEncodeCommit, ErrCommitEncodeCommit, txnId, err2)
		return
	}
	_, err2 = w.logAppender.Append(record.RecordTypeCommitTransaction, commitTxn)
	if err2 != nil {
		w.logger.Error("failed to append commit txn record", err2, "txn", txnId)
		err = wrapCommitErr(StageAppendCommit, ErrCommitAppendCommit, txnId, err2)
		return
	}

	if err2 = w.Flush(); err2 != nil {
		w.logger.Error("failed to flush WAL", err2, "txn", txnId)
		err = wrapCommitErr(StageFlush, ErrCommitFlush, txnId, err2)
		return
	}

	if w.opts.FsyncOnCommit {
		if err2 = w.FSync(); err2 != nil {
			w.logger.Error("failed to fsync WAL", err2, "txn", txnId)
			err = wrapCommitErr(StageFSync, ErrCommitFSync, txnId, err2)
			return
		}
	}

	w.logger.Info("commit successful", "txn", txnId, "count", opCount)
	return
}

func (w *Writer) Flush() error {
	return w.logAppender.Flush()
}

func (w *Writer) FSync() error {
	return w.logAppender.FSync()
}
