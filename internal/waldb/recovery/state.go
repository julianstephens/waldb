package recovery

import (
	"github.com/julianstephens/waldb/internal/waldb/memtable"
	"github.com/julianstephens/waldb/internal/waldb/wal/record"
)

type txnBuf struct {
	id  uint64
	ops []op
}

type op struct {
	kind  record.RecordType
	key   []byte
	value []byte // nil for delete
}

type replayState struct {
	inflight     map[uint64]*txnBuf
	maxCommitted uint64
	memtable     *memtable.Table
}

type recordCtx struct {
	offset      int64
	segId       uint64
	declaredLen uint32
	recordType  record.RecordType
}

// newReplayState creates a new replayState for use during WAL replay.
func newReplayState(mem *memtable.Table) *replayState {
	return &replayState{
		inflight:     make(map[uint64]*txnBuf),
		maxCommitted: 0,
		memtable:     mem,
	}
}

func (s *replayState) onBegin(payload record.BeginCommitTransactionPayload, ctx recordCtx) error {
	if payload.TxnID == 0 {
		return &ReplayLogicError{
			AtOffset: ctx.offset,
			Type:     ctx.recordType,
			TxnID:    payload.TxnID,
			Err:      ErrTxnMismatch,
		}
	}

	if _, exists := s.inflight[payload.TxnID]; exists {
		return &ReplayLogicError{
			AtOffset: ctx.offset,
			Type:     ctx.recordType,
			TxnID:    payload.TxnID,
			Err:      ErrDoubleBegin,
		}
	}

	s.inflight[payload.TxnID] = &txnBuf{id: payload.TxnID}

	return nil
}

func (s *replayState) onPut(payload record.PutOpPayload, ctx recordCtx) error {
	if _, exists := s.inflight[payload.TxnID]; !exists {
		return &ReplayLogicError{
			AtOffset: ctx.offset,
			Type:     ctx.recordType,
			TxnID:    payload.TxnID,
			Err:      ErrOrphanOp,
		}
	}

	s.inflight[payload.TxnID].ops = append(s.inflight[payload.TxnID].ops, op{
		kind:  ctx.recordType,
		key:   payload.Key,
		value: payload.Value,
	})

	return nil
}
func (s *replayState) onDel(payload record.DeleteOpPayload, ctx recordCtx) error {
	if _, exists := s.inflight[payload.TxnID]; !exists {
		return &ReplayLogicError{
			AtOffset: ctx.offset,
			Type:     ctx.recordType,
			TxnID:    payload.TxnID,
			Err:      ErrOrphanOp,
		}
	}

	s.inflight[payload.TxnID].ops = append(s.inflight[payload.TxnID].ops, op{
		kind:  ctx.recordType,
		key:   payload.Key,
		value: nil,
	})

	return nil
}

func (s *replayState) onCommit(payload record.BeginCommitTransactionPayload, ctx recordCtx) error {
	if _, exists := s.inflight[payload.TxnID]; !exists {
		return &ReplayLogicError{
			AtOffset: ctx.offset,
			Type:     ctx.recordType,
			TxnID:    payload.TxnID,
			Err:      ErrCommitNoTxn,
		}
	}

	if payload.TxnID <= s.maxCommitted {
		return &ReplayLogicError{
			AtOffset: ctx.offset,
			Type:     ctx.recordType,
			TxnID:    payload.TxnID,
			Err:      ErrTxnNotMonotonic,
		}
	}

	memOps := make([]memtable.Op, 0, len(s.inflight[payload.TxnID].ops))
	for _, op := range s.inflight[payload.TxnID].ops {
		switch op.kind {
		case record.RecordTypePutOperation:
			memOps = append(memOps, memtable.Op{
				Kind:  memtable.OpPut,
				Key:   op.key,
				Value: op.value,
			})
		case record.RecordTypeDeleteOperation:
			memOps = append(memOps, memtable.Op{
				Kind: memtable.OpDelete,
				Key:  op.key,
			})
		}
	}

	if err := s.memtable.Apply(memOps); err != nil {
		return &ReplayLogicError{
			AtOffset: ctx.offset,
			Type:     ctx.recordType,
			TxnID:    payload.TxnID,
			Err:      err,
		}
	}

	delete(s.inflight, payload.TxnID)

	if payload.TxnID > s.maxCommitted {
		s.maxCommitted = payload.TxnID
	}

	return nil
}
