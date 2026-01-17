package txn

import (
	"github.com/julianstephens/waldb/internal/waldb/wal"
	"github.com/julianstephens/waldb/internal/waldb/wal/record"
)

type WriterOpts struct {
	FsyncOnCommit bool
}

type Writer struct {
	idAllocator IDAllocator
	logSink     wal.LogWriter
	opts        WriterOpts
}

// NewWriter creates a new Writer that writes transactions to the given LogSink.
func NewWriter(allocator IDAllocator, logSink wal.LogWriter, opts WriterOpts) *Writer {
	return &Writer{
		idAllocator: allocator,
		logSink:     logSink,
		opts:        opts,
	}
}

// Commit writes a transaction consisting of the operations in the given batch to the WAL.
// It returns the assigned transaction ID upon success.
func (w *Writer) Commit(batch *Batch) (txnId uint64, err error) {
	txnId = w.idAllocator.Next()

	beginTxn, err2 := record.EncodeBeginTxnPayload(txnId)
	if err2 != nil {
		err = err2
		return
	}
	_, err = w.logSink.Append(record.RecordTypeBeginTransaction, beginTxn)
	if err != nil {
		return
	}

	for _, op := range batch.ops {
		switch op.Kind {
		case OpPut:
			putOp, err2 := record.EncodePutOpPayload(txnId, op.Key, op.Value)
			if err2 != nil {
				err = err2
				return
			}
			_, err = w.logSink.Append(record.RecordTypePutOperation, putOp)
			if err != nil {
				return
			}
		case OpDelete:
			deleteOp, err2 := record.EncodeDeleteOpPayload(txnId, op.Key)
			if err2 != nil {
				err = err2
				return
			}
			_, err = w.logSink.Append(record.RecordTypeDeleteOperation, deleteOp)
			if err != nil {
				return
			}
		default:
			err = &TxnIDError{
				Err:  ErrTxnInvalidOpKind,
				Have: uint64(op.Kind),
				Want: uint64(OpPut),
			}
			return
		}
	}

	commitTxn, err2 := record.EncodeCommitTxnPayload(txnId)
	if err2 != nil {
		err = err2
		return
	}
	_, err = w.logSink.Append(record.RecordTypeCommitTransaction, commitTxn)
	if err != nil {
		return
	}

	if err = w.Flush(); err != nil {
		return
	}

	if w.opts.FsyncOnCommit {
		err = w.FSync()
		if err != nil {
			return
		}
	}

	return
}

func (w *Writer) Flush() error {
	return w.logSink.Flush()
}

func (w *Writer) FSync() error {
	return w.logSink.FSync()
}
