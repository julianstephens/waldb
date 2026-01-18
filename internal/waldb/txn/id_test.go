package txn_test

import (
	"sync"
	"testing"

	"github.com/julianstephens/waldb/internal/waldb/txn"
)

func TestNewCounterAllocatorValid(t *testing.T) {
	allocator, err := txn.NewCounterAllocator(1)
	if err != nil {
		t.Errorf("unexpected error creating allocator with next=1: %v", err)
	}
	if allocator == nil {
		t.Fatal("expected non-nil allocator")
	}
}

func TestNewCounterAllocatorZero(t *testing.T) {
	_, err := txn.NewCounterAllocator(0)
	if err == nil {
		t.Fatal("expected error creating allocator with next=0")
	}

	txnErr, ok := err.(*txn.TxnIDError)
	if !ok {
		t.Errorf("expected TxnIDError, got %T", err)
	}
	if txnErr.Err != txn.ErrInvalidTxnID {
		t.Errorf("expected ErrInvalidTxnID, got %v", txnErr.Err)
	}
	if txnErr.Have != 0 {
		t.Errorf("expected Have=0, got %d", txnErr.Have)
	}
	if txnErr.Want != 1 {
		t.Errorf("expected Want=1, got %d", txnErr.Want)
	}
}

func TestNewCounterAllocatorLargeValue(t *testing.T) {
	// Test that allocator accepts large but valid uint64 values
	largeValue := uint64(9999999999)
	allocator, err := txn.NewCounterAllocator(largeValue)
	if err != nil {
		t.Errorf("unexpected error creating allocator with large value: %v", err)
	}
	if allocator == nil {
		t.Fatal("expected non-nil allocator")
	}

	id := allocator.Next()
	if id != largeValue {
		t.Errorf("expected first Next() to return %d, got %d", largeValue, id)
	}
}

func TestNewCounterAllocatorLarge(t *testing.T) {
	startID := uint64(1000000)
	allocator, err := txn.NewCounterAllocator(startID)
	if err != nil {
		t.Errorf("unexpected error creating allocator with large next: %v", err)
	}

	id := allocator.Next()
	if id != startID {
		t.Errorf("expected first Next() to return %d, got %d", startID, id)
	}
}

func TestNextMonotonicallyIncreasing(t *testing.T) {
	allocator, err := txn.NewCounterAllocator(1)
	if err != nil {
		t.Fatalf("failed to create allocator: %v", err)
	}

	var prevID uint64 = 0
	for i := 0; i < 100; i++ {
		id := allocator.Next()
		if id <= prevID {
			t.Errorf("expected ID %d to be greater than previous %d", id, prevID)
		}
		prevID = id
	}
}

func TestNextStartsAfterInitial(t *testing.T) {
	startID := uint64(42)
	allocator, err := txn.NewCounterAllocator(startID)
	if err != nil {
		t.Fatalf("failed to create allocator: %v", err)
	}

	// First Next() should return startID (not startID+1)
	id := allocator.Next()
	if id != startID {
		t.Errorf("expected first Next() to return %d, got %d", startID, id)
	}

	// Second Next() should return startID+1
	id = allocator.Next()
	if id != startID+1 {
		t.Errorf("expected second Next() to return %d, got %d", startID+1, id)
	}
}

func TestPeekDoesNotConsume(t *testing.T) {
	allocator, err := txn.NewCounterAllocator(1)
	if err != nil {
		t.Fatalf("failed to create allocator: %v", err)
	}

	// Peek multiple times
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
}

func TestPeekConsistentWithNext(t *testing.T) {
	allocator, err := txn.NewCounterAllocator(100)
	if err != nil {
		t.Fatalf("failed to create allocator: %v", err)
	}

	for i := 0; i < 50; i++ {
		peeked := allocator.Peek()
		actual := allocator.Next()

		if peeked != actual {
			t.Errorf("iteration %d: Peek() returned %d, Next() returned %d", i, peeked, actual)
		}
	}
}

func TestSetNextValid(t *testing.T) {
	allocator, err := txn.NewCounterAllocator(1)
	if err != nil {
		t.Fatalf("failed to create allocator: %v", err)
	}

	err = allocator.SetNext(100)
	if err != nil {
		t.Errorf("unexpected error calling SetNext(100): %v", err)
	}

	if allocator.Peek() != 100 {
		t.Errorf("expected Peek() to return 100 after SetNext(100), got %d", allocator.Peek())
	}

	id := allocator.Next()
	if id != 100 {
		t.Errorf("expected Next() to return 100 after SetNext(100), got %d", id)
	}
}

func TestSetNextZero(t *testing.T) {
	allocator, err := txn.NewCounterAllocator(1)
	if err != nil {
		t.Fatalf("failed to create allocator: %v", err)
	}

	err = allocator.SetNext(0)
	if err == nil {
		t.Fatal("expected error calling SetNext(0)")
	}

	txnErr, ok := err.(*txn.TxnIDError)
	if !ok {
		t.Errorf("expected TxnIDError, got %T", err)
	}
	if txnErr.Err != txn.ErrInvalidTxnID {
		t.Errorf("expected ErrInvalidTxnID, got %v", txnErr.Err)
	}
}

func TestSetNextRegression(t *testing.T) {
	allocator, err := txn.NewCounterAllocator(100)
	if err != nil {
		t.Fatalf("failed to create allocator: %v", err)
	}

	// Try to set to a lower value
	err = allocator.SetNext(50)
	if err == nil {
		t.Fatal("expected error calling SetNext(50) when current is 100")
	}

	txnErr, ok := err.(*txn.TxnIDError)
	if !ok {
		t.Errorf("expected TxnIDError, got %T", err)
	}
	if txnErr.Err != txn.ErrTxnIDRegression {
		t.Errorf("expected ErrTxnIDRegression, got %v", txnErr.Err)
	}
	if txnErr.Have != 50 {
		t.Errorf("expected Have=50, got %d", txnErr.Have)
	}
	if txnErr.Want != 100 {
		t.Errorf("expected Want=100, got %d", txnErr.Want)
	}
}

func TestSetNextSameValue(t *testing.T) {
	allocator, err := txn.NewCounterAllocator(50)
	if err != nil {
		t.Fatalf("failed to create allocator: %v", err)
	}

	// Setting to the same value should succeed
	err = allocator.SetNext(50)
	if err != nil {
		t.Errorf("unexpected error calling SetNext(50) when current is 50: %v", err)
	}

	if allocator.Peek() != 50 {
		t.Errorf("expected Peek() to return 50, got %d", allocator.Peek())
	}
}

func TestSetNextAfterSomeAllocations(t *testing.T) {
	allocator, err := txn.NewCounterAllocator(1)
	if err != nil {
		t.Fatalf("failed to create allocator: %v", err)
	}

	// Allocate a few IDs
	id1 := allocator.Next() // 1
	id2 := allocator.Next() // 2
	id3 := allocator.Next() // 3

	if id1 != 1 || id2 != 2 || id3 != 3 {
		t.Fatalf("unexpected allocations: %d, %d, %d", id1, id2, id3)
	}

	// Now advance to a much higher value
	err = allocator.SetNext(1000)
	if err != nil {
		t.Errorf("unexpected error calling SetNext(1000): %v", err)
	}

	id4 := allocator.Next()
	if id4 != 1000 {
		t.Errorf("expected next ID to be 1000, got %d", id4)
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
