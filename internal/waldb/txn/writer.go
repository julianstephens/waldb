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
		err = &CommitError{
			Err:   ErrCommitInvalidBatch,
			Stage: StageValidateBatch,
			Cause: err2,
		}
		return
	}

	txnId = w.idAllocator.Next()

	beginTxn, err2 := record.EncodeBeginTxnPayload(txnId)
	if err2 != nil {
		err = wrapCommitErr(StageEncodeBegin, ErrCommitEncodeBegin, txnId, err2)
		return
	}
	_, err2 = w.logAppender.Append(record.RecordTypeBeginTransaction, beginTxn)
	if err2 != nil {
		err = wrapCommitErr(StageAppendBegin, ErrCommitAppendBegin, txnId, err2)
		return
	}

	for i, op := range batch.Ops() {
		switch op.Kind {
		case kv.OpPut:
			putOp, err2 := record.EncodePutOpPayload(txnId, op.Key, op.Value)
			if err2 != nil {
				err = wrapCommitOpErr(StageEncodeOp, ErrCommitEncodeOp, txnId, i, op, err2)
				return
			}
			_, err2 = w.logAppender.Append(record.RecordTypePutOperation, putOp)
			if err2 != nil {
				err = wrapCommitOpErr(StageAppendOp, ErrCommitAppendOp, txnId, i, op, err2)
				return
			}
		case kv.OpDelete:
			deleteOp, err2 := record.EncodeDeleteOpPayload(txnId, op.Key)
			if err2 != nil {
				err = wrapCommitOpErr(StageEncodeOp, ErrCommitEncodeOp, txnId, i, op, err2)
				return
			}
			_, err2 = w.logAppender.Append(record.RecordTypeDeleteOperation, deleteOp)
			if err2 != nil {
				err = wrapCommitOpErr(StageAppendOp, ErrCommitAppendOp, txnId, i, op, err2)
				return
			}
		}
	}

	commitTxn, err2 := record.EncodeCommitTxnPayload(txnId)
	if err2 != nil {
		err = wrapCommitErr(StageEncodeCommit, ErrCommitEncodeCommit, txnId, err2)
		return
	}
	_, err2 = w.logAppender.Append(record.RecordTypeCommitTransaction, commitTxn)
	if err2 != nil {
		err = wrapCommitErr(StageAppendCommit, ErrCommitAppendCommit, txnId, err2)
		return
	}

	if err2 = w.Flush(); err2 != nil {
		err = wrapCommitErr(StageFlush, ErrCommitFlush, txnId, err2)
		return
	}

	if w.opts.FsyncOnCommit {
		if err2 = w.FSync(); err2 != nil {
			err = wrapCommitErr(StageFSync, ErrCommitFSync, txnId, err2)
			return
		}
	}

	return
}

func (w *Writer) Flush() error {
	return w.logAppender.Flush()
}

func (w *Writer) FSync() error {
	return w.logAppender.FSync()
}
