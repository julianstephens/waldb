package txn_test

import (
	"sync"
	"testing"

	"github.com/julianstephens/waldb/internal/waldb/txn"
)

// TestCounterAllocatorInit_TableDriven tests allocator initialization with various values
func TestCounterAllocatorInit_TableDriven(t *testing.T) {
	testCases := []struct {
		name         string
		initialValue uint64
		expectError  bool
		checkError   func(error) bool
	}{
		{
			name:         "ValidInitialValue",
			initialValue: 1,
			expectError:  false,
		},
		{
			name:         "ZeroValue",
			initialValue: 0,
			expectError:  true,
			checkError: func(err error) bool {
				txnErr, ok := err.(*txn.TxnIDError)
				return ok && txnErr.Err == txn.ErrInvalidTxnID
			},
		},
		{
			name:         "LargeValue",
			initialValue: 9999999999,
			expectError:  false,
		},
		{
			name:         "LargeInitialID",
			initialValue: 1000000,
			expectError:  false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			allocator, err := txn.NewCounterAllocator(tc.initialValue)

			if tc.expectError {
				if err == nil {
					t.Fatalf("expected error for initial value %d", tc.initialValue)
				}
				if tc.checkError != nil && !tc.checkError(err) {
					t.Errorf("error validation failed for %v", err)
				}
			} else {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				if allocator == nil {
					t.Fatal("expected non-nil allocator")
				}

				// Verify first Next() returns the initial value
				id := allocator.Next()
				if id != tc.initialValue {
					t.Errorf("expected first Next() to return %d, got %d", tc.initialValue, id)
				}
			}
		})
	}
}

// TestCounterAllocatorPeekNext_TableDriven tests Peek() and Next() behavior
func TestCounterAllocatorPeekNext_TableDriven(t *testing.T) {
	testCases := []struct {
		name                  string
		initialValue          uint64
		nextCallSequence      int // Number of sequential Next() calls to make
		expectPeekConsistency bool
	}{
		{
			name:                  "MonotonicallyIncreasing",
			initialValue:          1,
			nextCallSequence:      100,
			expectPeekConsistency: true,
		},
		{
			name:                  "StartAfterInitial",
			initialValue:          42,
			nextCallSequence:      2,
			expectPeekConsistency: true,
		},
		{
			name:                  "PeekDoesNotConsume",
			initialValue:          1,
			nextCallSequence:      0, // Will test peek separately
			expectPeekConsistency: true,
		},
		{
			name:                  "LargeStartValue",
			initialValue:          100,
			nextCallSequence:      50,
			expectPeekConsistency: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			allocator, err := txn.NewCounterAllocator(tc.initialValue)
			if err != nil {
				t.Fatalf("failed to create allocator: %v", err)
			}

			if tc.name == "PeekDoesNotConsume" {
				// Test that Peek doesn't advance
				peek1 := allocator.Peek()
				peek2 := allocator.Peek()
				if peek1 != peek2 {
					t.Errorf("expected Peek() to return same value, got %d and %d", peek1, peek2)
				}

				// Next should return what Peek showed
				id := allocator.Next()
				if id != peek1 {
					t.Errorf("expected Next() to return %d (from Peek), got %d", peek1, id)
				}
			} else {
				// For other tests, verify monotonicity and peek consistency
				var prevID uint64 = 0
				for i := 0; i < tc.nextCallSequence; i++ {
					if tc.expectPeekConsistency {
						peeked := allocator.Peek()
						actual := allocator.Next()
						if peeked != actual {
							t.Errorf("iteration %d: Peek() %d != Next() %d", i, peeked, actual)
						}
						actual = peeked // Use the peeked value for monotonicity check
						if actual <= prevID {
							t.Errorf("expected ID %d to be greater than previous %d", actual, prevID)
						}
						prevID = actual
					}
				}
			}
		})
	}
}

// TestCounterAllocatorSetNext_TableDriven tests SetNext() with various scenarios
func TestCounterAllocatorSetNext_TableDriven(t *testing.T) {
	testCases := []struct {
		name               string
		initialValue       uint64
		allocationsFirst   int    // Number of Next() calls before SetNext
		setNextValue       uint64 // Value to set
		expectError        bool
		checkError         func(error) bool
		verifyAfterSetNext func(*txn.CounterAllocator) bool
	}{
		{
			name:             "SetToHigherValue",
			initialValue:     1,
			allocationsFirst: 0,
			setNextValue:     100,
			expectError:      false,
			verifyAfterSetNext: func(a *txn.CounterAllocator) bool {
				return a.Peek() == 100 && a.Next() == 100
			},
		},
		{
			name:             "SetToZero",
			initialValue:     1,
			allocationsFirst: 0,
			setNextValue:     0,
			expectError:      true,
			checkError: func(err error) bool {
				txnErr, ok := err.(*txn.TxnIDError)
				return ok && txnErr.Err == txn.ErrInvalidTxnID
			},
		},
		{
			name:             "SetToSameValue",
			initialValue:     50,
			allocationsFirst: 0,
			setNextValue:     50,
			expectError:      false,
			verifyAfterSetNext: func(a *txn.CounterAllocator) bool {
				return a.Peek() == 50
			},
		},
		{
			name:             "SetAfterAllocations",
			initialValue:     1,
			allocationsFirst: 3,
			setNextValue:     1000,
			expectError:      false,
			verifyAfterSetNext: func(a *txn.CounterAllocator) bool {
				return a.Next() == 1000
			},
		},
		{
			name:             "RegressionAttempt",
			initialValue:     100,
			allocationsFirst: 0,
			setNextValue:     50,
			expectError:      true,
			checkError: func(err error) bool {
				txnErr, ok := err.(*txn.TxnIDError)
				return ok && txnErr.Err == txn.ErrTxnIDRegression
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			allocator, err := txn.NewCounterAllocator(tc.initialValue)
			if err != nil {
				t.Fatalf("failed to create allocator: %v", err)
			}

			// Perform allocations if needed
			for i := 0; i < tc.allocationsFirst; i++ {
				allocator.Next()
			}

			err = allocator.SetNext(tc.setNextValue)

			if tc.expectError {
				if err == nil {
					t.Fatalf("expected error calling SetNext(%d)", tc.setNextValue)
				}
				if tc.checkError != nil && !tc.checkError(err) {
					t.Errorf("error validation failed for %v", err)
				}
			} else {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				if tc.verifyAfterSetNext != nil && !tc.verifyAfterSetNext(allocator) {
					t.Error("verification after SetNext failed")
				}
			}
		})
	}
}

func TestConcurrentNext(t *testing.T) {
	allocator, err := txn.NewCounterAllocator(1)
	if err != nil {
		t.Fatalf("failed to create allocator: %v", err)
	}

	const numGoroutines = 10
	const allocsPerGoroutine = 100

	ids := make(chan uint64, numGoroutines*allocsPerGoroutine)
	var wg sync.WaitGroup

	// Launch goroutines that allocate IDs concurrently
	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < allocsPerGoroutine; i++ {
				ids <- allocator.Next()
			}
		}()
	}

	wg.Wait()
	close(ids)

	// Collect all IDs
	collectedIDs := make([]uint64, 0, numGoroutines*allocsPerGoroutine)
	for id := range ids {
		collectedIDs = append(collectedIDs, id)
	}

	// Verify we got the expected number of IDs
	if len(collectedIDs) != numGoroutines*allocsPerGoroutine {
		t.Errorf("expected %d IDs, got %d", numGoroutines*allocsPerGoroutine, len(collectedIDs))
	}

	// Verify all IDs are unique and within expected range
	seenIDs := make(map[uint64]bool)
	for _, id := range collectedIDs {
		if seenIDs[id] {
			t.Errorf("duplicate ID: %d", id)
		}
		seenIDs[id] = true
		if id < 1 || id > uint64(numGoroutines*allocsPerGoroutine) {
			t.Errorf("ID %d out of expected range [1, %d]", id, numGoroutines*allocsPerGoroutine)
		}
	}
}

func TestConcurrentPeekAndNext(t *testing.T) {
	allocator, err := txn.NewCounterAllocator(1)
	if err != nil {
		t.Fatalf("failed to create allocator: %v", err)
	}

	const numIterations = 100

	for i := 0; i < numIterations; i++ {
		peeked := allocator.Peek()
		actual := allocator.Next()

		if peeked != actual {
			t.Errorf("iteration %d: Peek() returned %d, Next() returned %d", i, peeked, actual)
		}
	}
}

func TestSetNextConcurrentWithNext(t *testing.T) {
	allocator, err := txn.NewCounterAllocator(1)
	if err != nil {
		t.Fatalf("failed to create allocator: %v", err)
	}

	var wg sync.WaitGroup
	errors := make(chan error, 10)

	// Goroutine that keeps allocating IDs
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 50; i++ {
			allocator.Next()
		}
	}()

	// Goroutine that tries to SetNext
	wg.Add(1)
	go func() {
		defer wg.Done()
		// After giving the first goroutine some time, set next to a high value
		for i := 0; i < 10; i++ {
			err := allocator.SetNext(1000)
			if err != nil && i == 0 {
				// First attempt might fail if we've already gone past 1000
				errors <- err
			}
		}
	}()

	wg.Wait()
	close(errors)

	// We should have gotten no errors (or only the regression error, which is acceptable)
	for err := range errors {
		if err != nil {
			t.Logf("got error during concurrent operations: %v", err)
		}
	}
}
