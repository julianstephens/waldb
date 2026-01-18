package recovery

import (
	"github.com/julianstephens/waldb/internal/waldb/kv"
	"github.com/julianstephens/waldb/internal/waldb/memtable"
	"github.com/julianstephens/waldb/internal/waldb/wal/record"
)

// Export unexported types and functions for testing

// TxnBuf wraps the internal txnBuf type with exported fields
type TxnBuf struct {
	ID  uint64
	Ops []kv.Op
}

// Op wraps the internal op type with exported fields
type Op struct {
	Kind  record.RecordType
	Key   []byte
	Value []byte // nil for delete
}

// ReplayState is an exported alias for the internal replayState
type ReplayState = replayState

type RecordCtx struct {
	Offset      int64
	SegId       uint64
	DeclaredLen uint32
	RecordType  record.RecordType
}

func NewRecordCtx(offset int64, segId uint64, declaredLen uint32, recordType record.RecordType) RecordCtx {
	return RecordCtx{
		Offset:      offset,
		SegId:       segId,
		DeclaredLen: declaredLen,
		RecordType:  recordType,
	}
}

func NewReplayState(mem *memtable.Table) *ReplayState {
	return newReplayState(mem)
}

func (s *ReplayState) OnBegin(payload record.BeginCommitTransactionPayload, ctx RecordCtx) error {
	internalCtx := recordCtx{
		offset:      ctx.Offset,
		segId:       ctx.SegId,
		declaredLen: ctx.DeclaredLen,
		recordType:  ctx.RecordType,
	}
	return s.onBegin(payload, internalCtx)
}

func (s *ReplayState) OnPut(payload record.PutOpPayload, ctx RecordCtx) error {
	internalCtx := recordCtx{
		offset:      ctx.Offset,
		segId:       ctx.SegId,
		declaredLen: ctx.DeclaredLen,
		recordType:  ctx.RecordType,
	}
	return s.onPut(payload, internalCtx)
}

func (s *ReplayState) OnDel(payload record.DeleteOpPayload, ctx RecordCtx) error {
	internalCtx := recordCtx{
		offset:      ctx.Offset,
		segId:       ctx.SegId,
		declaredLen: ctx.DeclaredLen,
		recordType:  ctx.RecordType,
	}
	return s.onDel(payload, internalCtx)
}

func (s *ReplayState) OnCommit(payload record.BeginCommitTransactionPayload, ctx RecordCtx) error {
	internalCtx := recordCtx{
		offset:      ctx.Offset,
		segId:       ctx.SegId,
		declaredLen: ctx.DeclaredLen,
		recordType:  ctx.RecordType,
	}
	return s.onCommit(payload, internalCtx)
}

// Inflight returns a copy of the inflight transactions with exported field names
func (s *ReplayState) Inflight() map[uint64]*TxnBuf {
	result := make(map[uint64]*TxnBuf)
	for txnID, internalBuf := range s.inflight {
		result[txnID] = &TxnBuf{
			ID:  internalBuf.id,
			Ops: internalBuf.ops,
		}
	}
	return result
}

func (s *ReplayState) MaxCommitted() uint64 {
	return s.maxCommitted
}
