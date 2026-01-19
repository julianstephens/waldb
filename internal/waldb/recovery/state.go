package recovery

import (
	"github.com/julianstephens/waldb/internal/waldb/kv"
	"github.com/julianstephens/waldb/internal/waldb/memtable"
	"github.com/julianstephens/waldb/internal/waldb/wal/record"
)

type txnState int

const (
	StateAbsent txnState = iota
	StateOpen
	StateCommitted
	StateIgnored
)

type txnBuf struct {
	id  uint64
	ops []kv.Op
}

type replayState struct {
	// map of inflight transactions being replayed with their buffered operations
	inflight map[uint64]*txnBuf
	// maximum committed transaction ID seen so far
	maxCommitted uint64
	// memtable to apply committed transactions to
	memtable *memtable.Table
	// map of transaction IDs to their terminal states
	stateMap map[uint64]txnState
}

// newReplayState creates a new replayState for use during WAL replay.
// The provided memtable is used to apply committed transactions.
// It initializes the inflight transaction map and sets the maximum committed
// transaction ID to zero.
func newReplayState(mem *memtable.Table) *replayState {
	return &replayState{
		inflight:     make(map[uint64]*txnBuf),
		maxCommitted: 0,
		memtable:     mem,
		stateMap:     make(map[uint64]txnState),
	}
}

func (s *replayState) onBegin(payload record.BeginCommitTransactionPayload) error {
	if payload.TxnID == 0 {
		return &StateError{
			Kind:  StateTxnMismatch,
			Op:    "BEGIN",
			TxnID: payload.TxnID,
		}
	}

	if _, exists := s.inflight[payload.TxnID]; exists {
		return &StateError{
			Kind:  StateDoubleBegin,
			Op:    "BEGIN",
			TxnID: payload.TxnID,
		}
	}

	if err := s.validateState(payload.TxnID, "BEGIN"); err != nil {
		return err
	}

	s.inflight[payload.TxnID] = &txnBuf{id: payload.TxnID}
	s.stateMap[payload.TxnID] = StateOpen

	return nil
}

func (s *replayState) onPut(payload record.PutOpPayload) error {
	if _, exists := s.inflight[payload.TxnID]; !exists {
		return &StateError{
			Kind:  StateOrphanOp,
			Op:    "PUT",
			TxnID: payload.TxnID,
		}
	}

	if err := s.validateState(payload.TxnID, "PUT"); err != nil {
		return err
	}

	s.inflight[payload.TxnID].ops = append(s.inflight[payload.TxnID].ops, kv.Op{
		Kind:  kv.OpPut,
		Key:   payload.Key,
		Value: payload.Value,
	})

	return nil
}

func (s *replayState) onDel(payload record.DeleteOpPayload) error {
	if _, exists := s.inflight[payload.TxnID]; !exists {
		return &StateError{
			Kind:  StateOrphanOp,
			Op:    "DEL",
			TxnID: payload.TxnID,
		}
	}

	if err := s.validateState(payload.TxnID, "DEL"); err != nil {
		return err
	}

	s.inflight[payload.TxnID].ops = append(s.inflight[payload.TxnID].ops, kv.Op{
		Kind:  kv.OpDelete,
		Key:   payload.Key,
		Value: nil,
	})

	return nil
}

func (s *replayState) onCommit(payload record.BeginCommitTransactionPayload) error {
	if _, exists := s.inflight[payload.TxnID]; !exists {
		return &StateError{
			Kind:  StateCommitNoTxn,
			Op:    "COMMIT",
			TxnID: payload.TxnID,
		}
	}

	if err := s.validateState(payload.TxnID, "COMMIT"); err != nil {
		return err
	}

	if payload.TxnID <= s.maxCommitted {
		return &StateError{
			Kind:      StateTxnNotMonotonic,
			Op:        "COMMIT",
			HaveTxnID: payload.TxnID,
			WantTxnID: s.maxCommitted + 1,
		}
	}

	if err := s.memtable.Apply(s.inflight[payload.TxnID].ops); err != nil {
		return err
	}

	delete(s.inflight, payload.TxnID)
	s.stateMap[payload.TxnID] = StateCommitted

	if payload.TxnID > s.maxCommitted {
		s.maxCommitted = payload.TxnID
	}

	return nil
}

func (s *replayState) validateState(txnId uint64, op string) error {
	if ts, exists := s.stateMap[txnId]; exists {
		switch ts {
		case StateCommitted:
			return &StateError{
				Kind:  StateAfterCommit,
				Op:    op,
				TxnID: txnId,
			}
		case StateAbsent:
			if op != "BEGIN" {
				return &StateError{
					Kind:  StateOrphanOp,
					Op:    op,
					TxnID: txnId,
				}
			}
		case StateOpen:
			if op == "BEGIN" {
				return &StateError{
					Kind:  StateDoubleBegin,
					Op:    op,
					TxnID: txnId,
				}
			}
		case StateIgnored:
			if op != "BEGIN" {
				return &StateError{
					Kind:  StateAfterCommit,
					Op:    op,
					TxnID: txnId,
				}
			}
		}
	}
	return nil
}
