package recovery

import (
	"github.com/julianstephens/go-utils/generic"
	"github.com/julianstephens/waldb/internal/waldb/kv"
	"github.com/julianstephens/waldb/internal/waldb/memtable"
	"github.com/julianstephens/waldb/internal/waldb/wal/record"
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
	// map of already committed transaction IDs
	committed map[uint64]bool
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
		committed:    make(map[uint64]bool),
	}
}

func (s *replayState) onBegin(payload record.BeginCommitTransactionPayload) error {
	if err := s.rejectInvalidOrNonMonotonicTxnID(payload.TxnID, "BEGIN"); err != nil {
		return err
	}

	if err := s.rejectIfCommitted(payload.TxnID, "BEGIN"); err != nil {
		return err
	}

	if s.isOpen(payload.TxnID) {
		return &StateError{
			Kind:  StateDoubleBegin,
			Op:    "BEGIN",
			TxnID: payload.TxnID,
		}
	}

	s.inflight[payload.TxnID] = &txnBuf{id: payload.TxnID}

	return nil
}

func (s *replayState) onPut(payload record.PutOpPayload) error {
	if payload.TxnID == 0 {
		return &StateError{
			Kind:  StateTxnInvalidID,
			Op:    "PUT",
			TxnID: payload.TxnID,
		}
	}

	if err := s.rejectIfCommitted(payload.TxnID, "PUT"); err != nil {
		return err
	}

	if err := s.requireOpen(payload.TxnID, "PUT"); err != nil {
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
	if payload.TxnID == 0 {
		return &StateError{
			Kind:  StateTxnInvalidID,
			Op:    "DEL",
			TxnID: payload.TxnID,
		}
	}

	if err := s.rejectIfCommitted(payload.TxnID, "DEL"); err != nil {
		return err
	}

	if err := s.requireOpen(payload.TxnID, "DEL"); err != nil {
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
	if payload.TxnID == 0 {
		return &StateError{
			Kind:  StateTxnInvalidID,
			Op:    "COMMIT",
			TxnID: payload.TxnID,
		}
	}

	if err := s.rejectIfCommitted(payload.TxnID, "COMMIT"); err != nil {
		return err
	}

	if err := s.requireOpen(payload.TxnID, "COMMIT"); err != nil {
		return err
	}

	if err := s.memtable.Apply(s.inflight[payload.TxnID].ops); err != nil {
		return err
	}

	delete(s.inflight, payload.TxnID)
	s.markCommitted(payload.TxnID)

	if payload.TxnID > s.maxCommitted {
		s.maxCommitted = payload.TxnID
	}

	return nil
}

func (s *replayState) rejectInvalidOrNonMonotonicTxnID(txnId uint64, op string) error {
	if txnId == 0 {
		return &StateError{
			Kind:  StateTxnInvalidID,
			Op:    op,
			TxnID: txnId,
		}
	}
	// monotonicity enforce relative to committed txns not inflight
	if txnId <= s.maxCommitted {
		return &StateError{
			Kind:      StateTxnNotMonotonic,
			Op:        op,
			HaveTxnID: txnId,
			WantTxnID: s.maxCommitted + 1,
		}
	}
	return nil
}

func (s *replayState) rejectIfCommitted(txnId uint64, op string) error {
	if s.isCommitted(txnId) {
		return &StateError{
			Kind:  generic.If(op == "COMMIT", StateDoubleCommit, StateAfterCommit),
			Op:    op,
			TxnID: txnId,
		}
	}
	return nil
}

func (s *replayState) requireOpen(txnId uint64, op string) error {
	if !s.isOpen(txnId) {
		return &StateError{
			Kind:  StateOrphanOp,
			Op:    op,
			TxnID: txnId,
		}
	}
	return nil
}

func (s *replayState) isCommitted(txnId uint64) bool {
	_, exists := s.committed[txnId]
	return exists
}

func (s *replayState) markCommitted(txnId uint64) {
	s.committed[txnId] = true
}

func (s *replayState) isOpen(txnId uint64) bool {
	_, exists := s.inflight[txnId]
	return exists
}
