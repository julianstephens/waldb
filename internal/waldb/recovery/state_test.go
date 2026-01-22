package recovery

import (
	"testing"

	tst "github.com/julianstephens/go-utils/tests"
	"github.com/julianstephens/waldb/internal/waldb/memtable"
	"github.com/julianstephens/waldb/internal/waldb/wal/record"
)

// StateTransitionTest encodes a single state transition test case
type StateTransitionTest struct {
	name          string
	txnID         uint64
	operation     func(*ReplayState, uint64) error
	expectedError StateErrorKind
	description   string
}

// TestStateTransitions_TableDriven tests all state transition rules in a table-driven format
//
// Invariants enforced:
// - Absent → PUT/DEL ⇒ OrphanOp
// - Absent → COMMIT ⇒ CommitNoTxn
// - Open → BEGIN ⇒ DoubleBegin
// - Committed → PUT/DEL ⇒ AfterCommit
// - Committed → COMMIT ⇒ DoubleCommit (via AfterCommit check)
// - Non-monotonic BEGIN ⇒ TxnNotMonotonic
// - Invalid txnID 0 ⇒ StateTxnInvalidID
func TestStateTransitions_TableDriven(t *testing.T) {
	tests := []StateTransitionTest{
		// Absent state tests
		{
			name:  "PUT on absent transaction",
			txnID: 1,
			operation: func(rs *ReplayState, txnID uint64) error {
				return rs.OnPut(record.PutOpPayload{TxnID: txnID, Key: []byte("k"), Value: []byte("v")})
			},
			expectedError: StateOrphanOp,
			description:   "Absent → PUT ⇒ OrphanOp",
		},
		{
			name:  "DEL on absent transaction",
			txnID: 1,
			operation: func(rs *ReplayState, txnID uint64) error {
				return rs.OnDel(record.DeleteOpPayload{TxnID: txnID, Key: []byte("k")})
			},
			expectedError: StateOrphanOp,
			description:   "Absent → DEL ⇒ OrphanOp",
		},
		{
			name:  "COMMIT on absent transaction",
			txnID: 1,
			operation: func(rs *ReplayState, txnID uint64) error {
				return rs.OnCommit(record.BeginCommitTransactionPayload{TxnID: txnID})
			},
			expectedError: StateOrphanOp, // requireOpen returns OrphanOp
			description:   "Absent → COMMIT ⇒ OrphanOp",
		},

		// Invalid TxnID tests
		{
			name:  "BEGIN with TxnID 0",
			txnID: 0,
			operation: func(rs *ReplayState, txnID uint64) error {
				return rs.OnBegin(record.BeginCommitTransactionPayload{TxnID: txnID})
			},
			expectedError: StateTxnInvalidID,
			description:   "TxnID 0 ⇒ StateTxnInvalidID",
		},
		{
			name:  "PUT with TxnID 0",
			txnID: 0,
			operation: func(rs *ReplayState, txnID uint64) error {
				return rs.OnPut(record.PutOpPayload{TxnID: txnID, Key: []byte("k"), Value: []byte("v")})
			},
			expectedError: StateTxnInvalidID,
			description:   "TxnID 0 ⇒ StateTxnInvalidID",
		},

		// Open state tests
		{
			name:  "Double BEGIN on same TxnID",
			txnID: 1,
			operation: func(rs *ReplayState, txnID uint64) error {
				return rs.OnBegin(record.BeginCommitTransactionPayload{TxnID: txnID})
			},
			expectedError: StateDoubleBegin,
			description:   "Open → BEGIN ⇒ DoubleBegin",
		},

		// Committed state tests
		{
			name:  "PUT after COMMIT",
			txnID: 1,
			operation: func(rs *ReplayState, txnID uint64) error {
				return rs.OnPut(record.PutOpPayload{TxnID: txnID, Key: []byte("k"), Value: []byte("v")})
			},
			expectedError: StateAfterCommit,
			description:   "Committed → PUT ⇒ AfterCommit",
		},
		{
			name:  "DEL after COMMIT",
			txnID: 1,
			operation: func(rs *ReplayState, txnID uint64) error {
				return rs.OnDel(record.DeleteOpPayload{TxnID: txnID, Key: []byte("k")})
			},
			expectedError: StateAfterCommit,
			description:   "Committed → DEL ⇒ AfterCommit",
		},
		{
			name:  "COMMIT after COMMIT",
			txnID: 1,
			operation: func(rs *ReplayState, txnID uint64) error {
				return rs.OnCommit(record.BeginCommitTransactionPayload{TxnID: txnID})
			},
			expectedError: StateDoubleCommit,
			description:   "Committed → COMMIT ⇒ DoubleCommit",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mem := memtable.New()
			rs := NewReplayState(mem)

			// For tests that require pre-state setup (Open or Committed):
			// - If testing Double BEGIN: BEGIN first to reach Open state
			// - If testing after Committed: BEGIN and COMMIT first
			if tt.expectedError == StateDoubleBegin {
				// Setup: reach Open state
				err := rs.OnBegin(record.BeginCommitTransactionPayload{TxnID: tt.txnID})
				tst.AssertNoError(t, err, "setup BEGIN should succeed")
			}

			if tt.expectedError == StateAfterCommit || tt.expectedError == StateDoubleCommit {
				// Setup: reach Committed state
				err := rs.OnBegin(record.BeginCommitTransactionPayload{TxnID: tt.txnID})
				tst.AssertNoError(t, err, "setup BEGIN should succeed")
				err = rs.OnCommit(record.BeginCommitTransactionPayload{TxnID: tt.txnID})
				tst.AssertNoError(t, err, "setup COMMIT should succeed")
			}

			// Execute the test operation
			err := tt.operation(rs, tt.txnID)

			// Verify error
			tst.AssertTrue(t, err != nil, "operation should error: "+tt.description)
			stateErr := GetStateError(err)
			tst.AssertNotNil(t, stateErr, "error should be StateError")
			tst.AssertEqual(t, stateErr.Kind, tt.expectedError, "error kind mismatch: "+tt.description)
		})
	}
}

// TestStateTransitions_Monotonicity tests monotonic TxnID enforcement
func TestStateTransitions_Monotonicity(t *testing.T) {
	tests := []struct {
		name     string
		sequence []uint64 // TxnIDs to BEGIN
		failAt   int      // Index at which BEGIN should fail (-1 for all succeed)
		wantErr  StateErrorKind
	}{
		{
			name:     "monotonic sequence 1, 2, 3",
			sequence: []uint64{1, 2, 3},
			failAt:   -1,
		},
		{
			name:     "skip from 1 to 3 (still monotonic)",
			sequence: []uint64{1, 3},
			failAt:   -1,
		},
		{
			name:     "non-monotonic: 2 after maxCommitted 2",
			sequence: []uint64{1, 2, 3, 2},
			failAt:   3,
			wantErr:  StateTxnNotMonotonic,
		},
		{
			name:     "reuse same TxnID before commit",
			sequence: []uint64{1, 2, 1},
			failAt:   2,
			wantErr:  StateTxnNotMonotonic,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mem := memtable.New()
			rs := NewReplayState(mem)

			for i, txnID := range tt.sequence {
				err := rs.OnBegin(record.BeginCommitTransactionPayload{TxnID: txnID})

				if tt.failAt == i {
					// Expected to fail
					tst.AssertTrue(t, err != nil, "BEGIN should fail at expected index")
					stateErr := GetStateError(err)
					tst.AssertNotNil(t, stateErr)
					tst.AssertEqual(t, stateErr.Kind, tt.wantErr)
					return // Stop here for this test
				}

				tst.AssertNoError(t, err, "BEGIN should succeed at current position")

				// Auto-commit to advance maxCommitted
				err = rs.OnCommit(record.BeginCommitTransactionPayload{TxnID: txnID})
				tst.AssertNoError(t, err)
			}

			// All should have succeeded
			tst.AssertTrue(t, tt.failAt == -1, "sequence should have failed but didn't")
		})
	}
}

// TestStateTransitions_BeginAfterCommitSameTxnID tests that BEGIN after COMMIT with same txnID
// returns StateTxnNotMonotonic (not DoubleBegin) because monotonicity check dominates
func TestStateTransitions_BeginAfterCommitSameTxnID(t *testing.T) {
	mem := memtable.New()
	rs := NewReplayState(mem)

	// Commit transaction 1
	err := rs.OnBegin(record.BeginCommitTransactionPayload{TxnID: 1})
	tst.AssertNoError(t, err, "first BEGIN should succeed")

	err = rs.OnCommit(record.BeginCommitTransactionPayload{TxnID: 1})
	tst.AssertNoError(t, err, "first COMMIT should succeed")

	// Verify maxCommitted is now 1
	tst.AssertEqual(t, rs.MaxCommitted(), uint64(1), "maxCommitted should be 1")

	// Try to BEGIN transaction 1 again - should fail with StateTxnNotMonotonic
	// because 1 <= maxCommitted (1), not DoubleBegin
	err = rs.OnBegin(record.BeginCommitTransactionPayload{TxnID: 1})
	tst.AssertTrue(t, err != nil, "BEGIN after COMMIT with same TxnID should error")
	stateErr := GetStateError(err)
	tst.AssertNotNil(t, stateErr, "error should be StateError")
	tst.AssertEqual(
		t,
		stateErr.Kind,
		StateTxnNotMonotonic,
		"error should be StateTxnNotMonotonic (monotonicity check dominates)",
	)
}

// TestStateTransitions_InvalidTxnIDDominates tests that TxnID==0 check dominates all other checks
// even after commits have occurred
func TestStateTransitions_InvalidTxnIDDominates(t *testing.T) {
	mem := memtable.New()
	rs := NewReplayState(mem)

	// Commit some transactions to advance maxCommitted
	for i := uint64(1); i <= 3; i++ {
		err := rs.OnBegin(record.BeginCommitTransactionPayload{TxnID: i})
		tst.AssertNoError(t, err)
		err = rs.OnCommit(record.BeginCommitTransactionPayload{TxnID: i})
		tst.AssertNoError(t, err)
	}

	// Verify maxCommitted is now 3
	tst.AssertEqual(t, rs.MaxCommitted(), uint64(3), "maxCommitted should be 3")

	// Try to PUT with TxnID==0 - should fail with StateTxnInvalidID
	// (not any other error that might apply if we got further in the checks)
	err := rs.OnPut(record.PutOpPayload{TxnID: 0, Key: []byte("key"), Value: []byte("value")})
	tst.AssertTrue(t, err != nil, "PUT with TxnID==0 should error")
	stateErr := GetStateError(err)
	tst.AssertNotNil(t, stateErr, "error should be StateError")
	tst.AssertEqual(
		t,
		stateErr.Kind,
		StateTxnInvalidID,
		"error should be StateTxnInvalidID (invalid ID check dominates)",
	)
}

// TestStateTransitions_Interleaving tests interleaved transactions with various commit orders
func TestStateTransitions_Interleaving(t *testing.T) {
	tests := []struct {
		name             string
		txnSequence      []uint64 // BEGIN order
		commitOrder      []uint64 // COMMIT order
		wantMaxCommitted uint64
		wantSuccess      bool
	}{
		{
			name:             "BEGIN 1, BEGIN 2, COMMIT 1, COMMIT 2",
			txnSequence:      []uint64{1, 2},
			commitOrder:      []uint64{1, 2},
			wantMaxCommitted: 2,
			wantSuccess:      true,
		},
		{
			name:             "BEGIN 1, BEGIN 2, COMMIT 2, COMMIT 1",
			txnSequence:      []uint64{1, 2},
			commitOrder:      []uint64{2, 1},
			wantMaxCommitted: 2,
			wantSuccess:      true,
		},
		{
			name:             "BEGIN 1, BEGIN 2, BEGIN 3, COMMIT 2, COMMIT 3, COMMIT 1",
			txnSequence:      []uint64{1, 2, 3},
			commitOrder:      []uint64{2, 3, 1},
			wantMaxCommitted: 3,
			wantSuccess:      true,
		},
		{
			name:             "Sparse IDs: BEGIN 1, BEGIN 5, BEGIN 3, COMMIT all in order",
			txnSequence:      []uint64{1, 5, 3},
			commitOrder:      []uint64{1, 5, 3},
			wantMaxCommitted: 5,
			wantSuccess:      true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mem := memtable.New()
			rs := NewReplayState(mem)

			// Execute BEGINs
			for _, txnID := range tt.txnSequence {
				err := rs.OnBegin(record.BeginCommitTransactionPayload{TxnID: txnID})
				tst.AssertNoError(t, err, "BEGIN should succeed")

				// Add a simple operation to each
				err = rs.OnPut(record.PutOpPayload{
					TxnID: txnID,
					Key:   []byte("key" + string(rune(txnID))),
					Value: []byte("value" + string(rune(txnID))),
				})
				tst.AssertNoError(t, err, "PUT should succeed")
			}

			// Execute COMMITs in specified order
			for _, txnID := range tt.commitOrder {
				err := rs.OnCommit(record.BeginCommitTransactionPayload{TxnID: txnID})
				if !tt.wantSuccess {
					tst.AssertTrue(t, err != nil, "COMMIT should fail as expected")
					return
				}
				tst.AssertNoError(t, err, "COMMIT should succeed")
			}

			// Verify maxCommitted
			tst.AssertEqual(t, rs.MaxCommitted(), tt.wantMaxCommitted, "maxCommitted should match")

			// Verify inflight is empty
			inflight := rs.Inflight()
			tst.AssertEqual(t, len(inflight), 0, "inflight should be empty after all commits")
		})
	}
}

// TestStateTransitions_MaxCommittedInvariant tests that maxCommitted is the maximum committed TxnID
// even when commits occur out of numeric order
func TestStateTransitions_MaxCommittedInvariant(t *testing.T) {
	tests := []struct {
		name             string
		commitSequence   []uint64 // TxnIDs to commit in order
		wantMaxCommitted uint64
	}{
		{
			name:             "sequential: 1, 2, 3",
			commitSequence:   []uint64{1, 2, 3},
			wantMaxCommitted: 3,
		},
		{
			name:             "reverse order: 3, 2, 1 (but begin in order 1, 2, 3)",
			commitSequence:   []uint64{3, 2, 1},
			wantMaxCommitted: 3,
		},
		{
			name:             "middle first: 2, 1, 3",
			commitSequence:   []uint64{2, 1, 3},
			wantMaxCommitted: 3,
		},
		{
			name:             "sparse: 5, 2, 10, 1",
			commitSequence:   []uint64{5, 2, 10, 1},
			wantMaxCommitted: 10,
		},
		{
			name:             "single txn: 42",
			commitSequence:   []uint64{42},
			wantMaxCommitted: 42,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mem := memtable.New()
			rs := NewReplayState(mem)

			// BEGIN all transactions first
			for _, txnID := range tt.commitSequence {
				err := rs.OnBegin(record.BeginCommitTransactionPayload{TxnID: txnID})
				tst.AssertNoError(t, err, "BEGIN should succeed")
			}

			// COMMIT in specified order
			for _, txnID := range tt.commitSequence {
				err := rs.OnCommit(record.BeginCommitTransactionPayload{TxnID: txnID})
				tst.AssertNoError(t, err, "COMMIT should succeed")

				// maxCommitted should always be at least the current TxnID
				// and should never decrease
				tst.AssertTrue(t, rs.MaxCommitted() >= txnID, "maxCommitted should be >= committed TxnID")
			}

			// Final maxCommitted should be the maximum seen
			tst.AssertEqual(
				t,
				rs.MaxCommitted(),
				tt.wantMaxCommitted,
				"final maxCommitted should be maximum of committed TxnIDs",
			)
		})
	}
}

// TestStateTransitions_OperationOrdering tests correct ordering of operations within a transaction
func TestStateTransitions_OperationOrdering(t *testing.T) {
	mem := memtable.New()
	rs := NewReplayState(mem)

	// BEGIN transaction
	err := rs.OnBegin(record.BeginCommitTransactionPayload{TxnID: 1})
	tst.AssertNoError(t, err)

	// DEL then PUT same key
	err = rs.OnDel(record.DeleteOpPayload{TxnID: 1, Key: []byte("key1")})
	tst.AssertNoError(t, err)

	err = rs.OnPut(record.PutOpPayload{TxnID: 1, Key: []byte("key1"), Value: []byte("value1")})
	tst.AssertNoError(t, err)

	// COMMIT
	err = rs.OnCommit(record.BeginCommitTransactionPayload{TxnID: 1})
	tst.AssertNoError(t, err)

	// Verify final value
	val, found := mem.Get([]byte("key1"))
	tst.AssertTrue(t, found, "key1 should exist")
	tst.AssertDeepEqual(t, val, []byte("value1"))
}

// TestStateTransitions_EmptyTransaction tests committing a transaction with no operations
func TestStateTransitions_EmptyTransaction(t *testing.T) {
	mem := memtable.New()
	rs := NewReplayState(mem)

	// BEGIN and COMMIT with no operations
	err := rs.OnBegin(record.BeginCommitTransactionPayload{TxnID: 1})
	tst.AssertNoError(t, err)

	err = rs.OnCommit(record.BeginCommitTransactionPayload{TxnID: 1})
	tst.AssertNoError(t, err, "empty transaction should commit successfully")

	// Verify state
	inflight := rs.Inflight()
	tst.AssertEqual(t, len(inflight), 0, "inflight should be empty")
	tst.AssertEqual(t, rs.MaxCommitted(), uint64(1))
}

// TestStateTransitions_IgnoredTransaction tests that uncommitted transactions are not applied
func TestStateTransitions_IgnoredTransaction(t *testing.T) {
	mem := memtable.New()
	rs := NewReplayState(mem)

	// BEGIN and add operations but never COMMIT
	err := rs.OnBegin(record.BeginCommitTransactionPayload{TxnID: 1})
	tst.AssertNoError(t, err)

	err = rs.OnPut(record.PutOpPayload{TxnID: 1, Key: []byte("key1"), Value: []byte("value1")})
	tst.AssertNoError(t, err)

	// End of replay without COMMIT
	inflight := rs.Inflight()
	tst.AssertEqual(t, len(inflight), 1, "transaction should still be inflight")

	// Verify operation was NOT applied
	_, found := mem.Get([]byte("key1"))
	tst.AssertFalse(t, found, "key1 should NOT exist (transaction not committed)")
}

// ============================================================================
// SEMANTIC CORRUPTION TRUTH TABLE TEST
// ============================================================================

// SemanticCorruptionCase encodes a semantic corruption scenario from DESIGN.md Table B
type SemanticCorruptionCase struct {
	name              string
	txnID             uint64
	preCondition      func(*ReplayState, uint64) error // Setup state
	operation         func(*ReplayState, uint64) error // Attempt forbidden operation
	expectedErrorKind StateErrorKind                   // Must error immediately
	tableRef          string                           // Reference to Table B row
}

// TestStateTransitions_SemanticCorruptionTruthTable validates Table B (semantic corruption)
// from DESIGN.md: all invalid state transitions must error immediately with correct ErrorKind.
//
// Table B from DESIGN.md:
//
//	| Current state | Next record          | Classification          | Required behavior               |
//	| ------------- | -------------------- | ----------------------- | ------------------------------- |
//	| Absent        | `PUT/DEL(txn_id, …)` | **Orphan op**           | **Stop recovery** (invalid WAL) |
//	| Absent        | `COMMIT_TXN(txn_id)` | **Orphan op**           | **Stop recovery**               |
//	| Open          | `BEGIN_TXN(txn_id)`  | **Duplicate begin**     | **Stop recovery**               |
//	| Committed     | `PUT/DEL(txn_id, …)` | **After-commit record** | **Stop recovery**               |
//	| Committed     | `COMMIT_TXN(txn_id)` | **Double commit**       | **Stop recovery**               |
func TestStateTransitions_SemanticCorruptionTruthTable(t *testing.T) {
	tests := []SemanticCorruptionCase{
		// ===== Absent state (no txn_id in inflight) =====
		{
			name:         "Absent → PUT (orphan op)",
			txnID:        1,
			preCondition: func(rs *ReplayState, txnID uint64) error { return nil }, // Absent = no setup
			operation: func(rs *ReplayState, txnID uint64) error {
				return rs.OnPut(record.PutOpPayload{TxnID: txnID, Key: []byte("k"), Value: []byte("v")})
			},
			expectedErrorKind: StateOrphanOp,
			tableRef:          "Table B row 1: Absent → PUT/DEL",
		},
		{
			name:         "Absent → DEL (orphan op)",
			txnID:        2,
			preCondition: func(rs *ReplayState, txnID uint64) error { return nil }, // Absent = no setup
			operation: func(rs *ReplayState, txnID uint64) error {
				return rs.OnDel(record.DeleteOpPayload{TxnID: txnID, Key: []byte("k")})
			},
			expectedErrorKind: StateOrphanOp,
			tableRef:          "Table B row 1: Absent → PUT/DEL",
		},
		{
			name:         "Absent → COMMIT (orphan op)",
			txnID:        3,
			preCondition: func(rs *ReplayState, txnID uint64) error { return nil }, // Absent = no setup
			operation: func(rs *ReplayState, txnID uint64) error {
				return rs.OnCommit(record.BeginCommitTransactionPayload{TxnID: txnID})
			},
			expectedErrorKind: StateOrphanOp,
			tableRef:          "Table B row 2: Absent → COMMIT_TXN",
		},

		// ===== Open state (txn_id in inflight, not yet committed) =====
		{
			name:  "Open → BEGIN (duplicate begin)",
			txnID: 1,
			preCondition: func(rs *ReplayState, txnID uint64) error {
				return rs.OnBegin(record.BeginCommitTransactionPayload{TxnID: txnID})
			},
			operation: func(rs *ReplayState, txnID uint64) error {
				return rs.OnBegin(record.BeginCommitTransactionPayload{TxnID: txnID})
			},
			expectedErrorKind: StateDoubleBegin,
			tableRef:          "Table B row 3: Open → BEGIN_TXN",
		},

		// ===== Committed state (txn_id has been committed) =====
		{
			name:  "Committed → PUT (after-commit record)",
			txnID: 1,
			preCondition: func(rs *ReplayState, txnID uint64) error {
				if err := rs.OnBegin(record.BeginCommitTransactionPayload{TxnID: txnID}); err != nil {
					return err
				}
				return rs.OnCommit(record.BeginCommitTransactionPayload{TxnID: txnID})
			},
			operation: func(rs *ReplayState, txnID uint64) error {
				return rs.OnPut(record.PutOpPayload{TxnID: txnID, Key: []byte("k"), Value: []byte("v")})
			},
			expectedErrorKind: StateAfterCommit,
			tableRef:          "Table B row 4: Committed → PUT/DEL",
		},
		{
			name:  "Committed → DEL (after-commit record)",
			txnID: 2,
			preCondition: func(rs *ReplayState, txnID uint64) error {
				if err := rs.OnBegin(record.BeginCommitTransactionPayload{TxnID: txnID}); err != nil {
					return err
				}
				return rs.OnCommit(record.BeginCommitTransactionPayload{TxnID: txnID})
			},
			operation: func(rs *ReplayState, txnID uint64) error {
				return rs.OnDel(record.DeleteOpPayload{TxnID: txnID, Key: []byte("k")})
			},
			expectedErrorKind: StateAfterCommit,
			tableRef:          "Table B row 4: Committed → PUT/DEL",
		},
		{
			name:  "Committed → COMMIT (double commit)",
			txnID: 3,
			preCondition: func(rs *ReplayState, txnID uint64) error {
				if err := rs.OnBegin(record.BeginCommitTransactionPayload{TxnID: txnID}); err != nil {
					return err
				}
				return rs.OnCommit(record.BeginCommitTransactionPayload{TxnID: txnID})
			},
			operation: func(rs *ReplayState, txnID uint64) error {
				return rs.OnCommit(record.BeginCommitTransactionPayload{TxnID: txnID})
			},
			expectedErrorKind: StateDoubleCommit,
			tableRef:          "Table B row 5: Committed → COMMIT_TXN",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rs := newReplayState(memtable.New())

			// Apply pre-condition to set up state
			if err := tc.preCondition(rs, tc.txnID); err != nil {
				t.Fatalf("precondition failed: %v", err)
			}

			// Attempt forbidden operation
			err := tc.operation(rs, tc.txnID)

			// Must error immediately
			tst.AssertNotNil(t, err, "expected error for semantic corruption ("+tc.tableRef+")")

			// Error must be of correct kind
			stateErr := GetStateError(err)
			if stateErr == nil {
				t.Fatalf("expected StateError but got: %T: %v", err, err)
			}
			tst.AssertEqual(
				t,
				stateErr.Kind,
				tc.expectedErrorKind,
				"error kind mismatch for "+tc.tableRef,
			)
		})
	}
}
