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

func NewReplayState(mem *memtable.Table) *ReplayState {
	return newReplayState(mem)
}

func (s *ReplayState) OnBegin(payload record.BeginCommitTransactionPayload) error {
	return s.onBegin(payload)
}

func (s *ReplayState) OnPut(payload record.PutOpPayload) error {
	return s.onPut(payload)
}

func (s *ReplayState) OnDel(payload record.DeleteOpPayload) error {
	return s.onDel(payload)
}

func (s *ReplayState) OnCommit(payload record.BeginCommitTransactionPayload) error {
	return s.onCommit(payload)
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
