package recovery

import (
	"github.com/julianstephens/waldb/internal/waldb/errorutil"
	"github.com/julianstephens/waldb/internal/waldb/kv"
	"github.com/julianstephens/waldb/internal/waldb/memtable"
	"github.com/julianstephens/waldb/internal/waldb/wal/record"
)

type txnBuf struct {
	id  uint64
	ops []kv.Op
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
			Coordinates: &errorutil.Coordinates{
				SegId:  &ctx.segId,
				Offset: &ctx.offset,
				TxnID:  &payload.TxnID,
			},
			Type: ctx.recordType,
			Err:  ErrTxnMismatch,
		}
	}

	if _, exists := s.inflight[payload.TxnID]; exists {
		return &ReplayLogicError{
			Coordinates: &errorutil.Coordinates{
				SegId:  &ctx.segId,
				Offset: &ctx.offset,
				TxnID:  &payload.TxnID,
			},
			Type: ctx.recordType,
			Err:  ErrDoubleBegin,
		}
	}

	s.inflight[payload.TxnID] = &txnBuf{id: payload.TxnID}

	return nil
}

func (s *replayState) onPut(payload record.PutOpPayload, ctx recordCtx) error {
	if _, exists := s.inflight[payload.TxnID]; !exists {
		return &ReplayLogicError{
			Coordinates: &errorutil.Coordinates{
				SegId:  &ctx.segId,
				Offset: &ctx.offset,
				TxnID:  &payload.TxnID,
			},
			Type: ctx.recordType,
			Err:  ErrOrphanOp,
		}
	}

	s.inflight[payload.TxnID].ops = append(s.inflight[payload.TxnID].ops, kv.Op{
		Kind:  kv.OpPut,
		Key:   payload.Key,
		Value: payload.Value,
	})

	return nil
}
func (s *replayState) onDel(payload record.DeleteOpPayload, ctx recordCtx) error {
	if _, exists := s.inflight[payload.TxnID]; !exists {
		return &ReplayLogicError{
			Coordinates: &errorutil.Coordinates{
				SegId:  &ctx.segId,
				Offset: &ctx.offset,
				TxnID:  &payload.TxnID,
			},
			Type: ctx.recordType,
			Err:  ErrOrphanOp,
		}
	}

	s.inflight[payload.TxnID].ops = append(s.inflight[payload.TxnID].ops, kv.Op{
		Kind:  kv.OpDelete,
		Key:   payload.Key,
		Value: nil,
	})

	return nil
}

func (s *replayState) onCommit(payload record.BeginCommitTransactionPayload, ctx recordCtx) error {
	if _, exists := s.inflight[payload.TxnID]; !exists {
		return &ReplayLogicError{
			Coordinates: &errorutil.Coordinates{
				SegId:  &ctx.segId,
				Offset: &ctx.offset,
				TxnID:  &payload.TxnID,
			},
			Type: ctx.recordType,
			Err:  ErrCommitNoTxn,
		}
	}

	if payload.TxnID <= s.maxCommitted {
		return &ReplayLogicError{
			Coordinates: &errorutil.Coordinates{
				SegId:  &ctx.segId,
				Offset: &ctx.offset,
				TxnID:  &payload.TxnID,
			},
			Type: ctx.recordType,
			Err:  ErrTxnNotMonotonic,
		}
	}

	if err := s.memtable.Apply(s.inflight[payload.TxnID].ops); err != nil {
		return &ReplayLogicError{
			Coordinates: &errorutil.Coordinates{
				SegId:  &ctx.segId,
				Offset: &ctx.offset,
				TxnID:  &payload.TxnID,
			},
			Type: ctx.recordType,
			Err:  err,
		}
	}

	delete(s.inflight, payload.TxnID)

	if payload.TxnID > s.maxCommitted {
		s.maxCommitted = payload.TxnID
	}

	return nil
}
