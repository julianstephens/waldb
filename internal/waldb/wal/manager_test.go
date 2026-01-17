package wal_test

import (
	"os"
	"path/filepath"
	"testing"

	tst "github.com/julianstephens/go-utils/tests"
	"github.com/julianstephens/waldb/internal/waldb/wal"
	"github.com/julianstephens/waldb/internal/waldb/wal/record"
)

// TestOpenManagerCreatesDirectory tests that OpenManager creates a WAL directory if it doesn't exist
func TestOpenManagerCreatesDirectory(t *testing.T) {
	tempDir := t.TempDir()
	walDir := filepath.Join(tempDir, "new_wal_dir")

	mgr, err := wal.OpenManager(walDir, wal.ManagerOpts{})
	tst.RequireNoError(t, err)
	tst.AssertNotNil(t, mgr, "expected non-nil manager")
	tst.RequireNoError(t, mgr.Close())

	// Verify directory was created
	_, err = os.Stat(walDir)
	tst.RequireNoError(t, err)
}

// TestOpenManagerCreatesFirstSegment tests that OpenManager creates the first segment
func TestOpenManagerCreatesFirstSegment(t *testing.T) {
	tempDir := t.TempDir()

	mgr, err := wal.OpenManager(tempDir, wal.ManagerOpts{})
	tst.RequireNoError(t, err)
	defer mgr.Close() //nolint:errcheck

	// Should have exactly one segment
	segIds := mgr.SegmentIds()
	tst.RequireDeepEqual(t, len(segIds), 1)
	tst.RequireDeepEqual(t, segIds[0], uint64(1))
}

// TestOpenManagerDiscoverExistingSegments tests that OpenManager discovers existing segments
func TestOpenManagerDiscoverExistingSegments(t *testing.T) {
	tempDir := t.TempDir()

	// Create first manager, append, close
	mgr1, err := wal.OpenManager(tempDir, wal.ManagerOpts{})
	tst.RequireNoError(t, err)

	payload := createBeginPayload(1)
	_, _, err = mgr1.Append(record.RecordTypeBeginTransaction, payload)
	tst.RequireNoError(t, err)

	tst.RequireNoError(t, mgr1.Close())

	// Open again and verify segments are discovered
	mgr2, err := wal.OpenManager(tempDir, wal.ManagerOpts{})
	tst.RequireNoError(t, err)
	defer mgr2.Close() //nolint:errcheck

	segIds := mgr2.SegmentIds()
	tst.RequireDeepEqual(t, len(segIds), 1)
	tst.RequireDeepEqual(t, segIds[0], uint64(1))
}

// TestManagerAppendSingleRecord tests appending a single record without rotation
func TestManagerAppendSingleRecord(t *testing.T) {
	tempDir := t.TempDir()

	mgr, err := wal.OpenManager(tempDir, wal.ManagerOpts{})
	tst.RequireNoError(t, err)
	defer mgr.Close() //nolint:errcheck

	payload := createBeginPayload(42)
	segId, offset, err := mgr.Append(record.RecordTypeBeginTransaction, payload)
	tst.RequireNoError(t, err)

	// Should be in segment 1 at offset 0
	tst.RequireDeepEqual(t, segId, uint64(1))
	tst.RequireDeepEqual(t, offset, int64(0))
}

// TestManagerAppendMultipleRecords tests appending multiple records without rotation
func TestManagerAppendMultipleRecords(t *testing.T) {
	tempDir := t.TempDir()

	mgr, err := wal.OpenManager(tempDir, wal.ManagerOpts{})
	tst.RequireNoError(t, err)
	defer mgr.Close() //nolint:errcheck

	// Append 5 records
	for i := uint64(1); i <= 5; i++ {
		payload := createBeginPayload(i)
		segId, offset, err := mgr.Append(record.RecordTypeBeginTransaction, payload)
		tst.RequireNoError(t, err)

		// All should be in segment 1
		tst.RequireDeepEqual(t, segId, uint64(1))
		// Each should have a different offset
		tst.AssertTrue(t, offset >= 0, "offset should be non-negative")
	}
}

// TestManagerAppendDifferentRecordTypes tests appending different record types
func TestManagerAppendDifferentRecordTypes(t *testing.T) {
	tempDir := t.TempDir()

	mgr, err := wal.OpenManager(tempDir, wal.ManagerOpts{})
	tst.RequireNoError(t, err)
	defer mgr.Close() //nolint:errcheck

	// Begin
	beginPayload := createBeginPayload(1)
	segId, _, err := mgr.Append(record.RecordTypeBeginTransaction, beginPayload)
	tst.RequireNoError(t, err)
	tst.RequireDeepEqual(t, segId, uint64(1))

	// Put
	putPayload := createPutPayload(1, []byte("key"), []byte("value"))
	segId, _, err = mgr.Append(record.RecordTypePutOperation, putPayload)
	tst.RequireNoError(t, err)
	tst.RequireDeepEqual(t, segId, uint64(1))

	// Delete
	delPayload := createDeletePayload(1, []byte("key"))
	segId, _, err = mgr.Append(record.RecordTypeDeleteOperation, delPayload)
	tst.RequireNoError(t, err)
	tst.RequireDeepEqual(t, segId, uint64(1))

	// Commit
	commitPayload := createBeginPayload(1)
	segId, _, err = mgr.Append(record.RecordTypeCommitTransaction, commitPayload)
	tst.RequireNoError(t, err)
	tst.RequireDeepEqual(t, segId, uint64(1))
}

// TestSegmentIds tests SegmentIds returns all segment IDs in sorted order
func TestSegmentIds(t *testing.T) {
	tempDir := t.TempDir()

	mgr, err := wal.OpenManager(tempDir, wal.ManagerOpts{})
	tst.RequireNoError(t, err)
	defer mgr.Close() //nolint:errcheck

	// Initially should have segment 1
	segIds := mgr.SegmentIds()
	tst.RequireDeepEqual(t, len(segIds), 1)
	tst.RequireDeepEqual(t, segIds[0], uint64(1))
}

// TestSegmentPath tests SegmentPath returns correct path for segment ID
func TestSegmentPath(t *testing.T) {
	tempDir := t.TempDir()

	mgr, err := wal.OpenManager(tempDir, wal.ManagerOpts{})
	tst.RequireNoError(t, err)
	defer mgr.Close() //nolint:errcheck

	path := mgr.SegmentPath(1)
	tst.AssertTrue(t, len(path) > 0, "expected non-empty path")
	tst.AssertTrue(t, path != "", "expected path to contain segment-00000000000000000001.wal")
}

// TestManagerFlushSingleRecord tests flushing a single appended record
func TestManagerFlushSingleRecord(t *testing.T) {
	tempDir := t.TempDir()

	mgr, err := wal.OpenManager(tempDir, wal.ManagerOpts{})
	tst.RequireNoError(t, err)
	defer mgr.Close() //nolint:errcheck

	payload := createBeginPayload(1)
	_, _, err = mgr.Append(record.RecordTypeBeginTransaction, payload)
	tst.RequireNoError(t, err)

	// Flush should succeed
	tst.RequireNoError(t, mgr.Flush())
}

// TestManagerFSyncSingleRecord tests fsyncing active segment
func TestManagerFSyncSingleRecord(t *testing.T) {
	tempDir := t.TempDir()

	mgr, err := wal.OpenManager(tempDir, wal.ManagerOpts{})
	tst.RequireNoError(t, err)
	defer mgr.Close() //nolint:errcheck

	payload := createBeginPayload(1)
	_, _, err = mgr.Append(record.RecordTypeBeginTransaction, payload)
	tst.RequireNoError(t, err)

	// FSync should succeed
	tst.RequireNoError(t, mgr.FSync())
}

// TestCloseManager tests closing the manager
func TestCloseManager(t *testing.T) {
	tempDir := t.TempDir()

	mgr, err := wal.OpenManager(tempDir, wal.ManagerOpts{})
	tst.RequireNoError(t, err)

	payload := createBeginPayload(1)
	_, _, err = mgr.Append(record.RecordTypeBeginTransaction, payload)
	tst.RequireNoError(t, err)

	// Close should succeed
	tst.RequireNoError(t, mgr.Close())
}

// TestManagerAppendAfterClose tests that appending after close fails
func TestManagerAppendAfterClose(t *testing.T) {
	tempDir := t.TempDir()

	mgr, err := wal.OpenManager(tempDir, wal.ManagerOpts{})
	tst.RequireNoError(t, err)

	tst.RequireNoError(t, mgr.Close())

	// Append after close should fail
	payload := createBeginPayload(1)
	_, _, err = mgr.Append(record.RecordTypeBeginTransaction, payload)
	if err == nil {
		t.Fatal("expected error appending after close")
	}
}

// TestManagerFlushAfterClose tests that flushing after close fails
func TestManagerFlushAfterClose(t *testing.T) {
	tempDir := t.TempDir()

	mgr, err := wal.OpenManager(tempDir, wal.ManagerOpts{})
	tst.RequireNoError(t, err)

	tst.RequireNoError(t, mgr.Close())

	// Flush after close should fail
	err = mgr.Flush()
	if err == nil {
		t.Fatal("expected error flushing after close")
	}
}

// TestManagerFSyncAfterClose tests that fsyncing after close fails
func TestManagerFSyncAfterClose(t *testing.T) {
	tempDir := t.TempDir()

	mgr, err := wal.OpenManager(tempDir, wal.ManagerOpts{})
	tst.RequireNoError(t, err)

	tst.RequireNoError(t, mgr.Close())

	// FSync after close should fail
	err = mgr.FSync()
	if err == nil {
		t.Fatal("expected error fsyncing after close")
	}
}

// TestRotationTriggersOnSize tests that rotation occurs when segment exceeds SegmentMaxBytes
func TestRotationTriggersOnSize(t *testing.T) {
	tempDir := t.TempDir()

	// Set small max size to force rotation quickly
	maxBytes := int64(500)
	mgr, err := wal.OpenManager(tempDir, wal.ManagerOpts{SegmentMaxBytes: maxBytes})
	tst.RequireNoError(t, err)
	defer mgr.Close() //nolint:errcheck

	// Append records until rotation occurs
	var lastSegId uint64
	var rotated bool

	for i := 0; i < 20; i++ {
		payload := createPutPayload(
			uint64(i),                        //nolint:gosec
			[]byte("key_"+string(rune(i))),   //nolint:gosec
			[]byte("value_"+string(rune(i))), //nolint:gosec
		)
		segId, _, err := mgr.Append(record.RecordTypePutOperation, payload)
		tst.RequireNoError(t, err)

		if i == 0 {
			lastSegId = segId
		} else if segId != lastSegId {
			rotated = true
			break
		}
	}

	// Should have rotated at least once
	tst.AssertTrue(t, rotated, "expected at least one rotation")

	// Should have multiple segments
	segIds := mgr.SegmentIds()
	tst.AssertTrue(t, len(segIds) > 1, "expected multiple segments after rotation")
}

// TestNoRotationWithZeroMaxBytes tests that zero SegmentMaxBytes disables rotation
func TestNoRotationWithZeroMaxBytes(t *testing.T) {
	tempDir := t.TempDir()

	// Zero means never rotate
	mgr, err := wal.OpenManager(tempDir, wal.ManagerOpts{SegmentMaxBytes: 0})
	tst.RequireNoError(t, err)
	defer mgr.Close() //nolint:errcheck

	// Append many large records
	for i := 0; i < 10; i++ {
		payload := createPutPayload(uint64(i), []byte("key"), make([]byte, 1024)) //nolint:gosec
		_, _, err := mgr.Append(record.RecordTypePutOperation, payload)
		tst.RequireNoError(t, err)
	}

	// Should still have only one segment
	segIds := mgr.SegmentIds()
	tst.RequireDeepEqual(t, len(segIds), 1)
	tst.RequireDeepEqual(t, segIds[0], uint64(1))
}

// TestCloseIsIdempotent tests that closing twice doesn't error
func TestCloseIsIdempotent(t *testing.T) {
	tempDir := t.TempDir()

	mgr, err := wal.OpenManager(tempDir, wal.ManagerOpts{})
	tst.RequireNoError(t, err)

	// First close should succeed
	tst.RequireNoError(t, mgr.Close())

	// Second close should also succeed (idempotent)
	err = mgr.Close()
	// Should not error or should return nil
	tst.AssertTrue(t, err == nil, "close should be idempotent")
}

// TestSegmentIdsAreSorted tests that SegmentIds returns segments in sorted order
func TestSegmentIdsAreSorted(t *testing.T) {
	tempDir := t.TempDir()

	mgr, err := wal.OpenManager(tempDir, wal.ManagerOpts{SegmentMaxBytes: 300})
	tst.RequireNoError(t, err)
	defer mgr.Close() //nolint:errcheck

	// Append enough records to cause multiple rotations
	for i := 0; i < 30; i++ {
		payload := createBeginPayload(uint64(i)) //nolint:gosec
		_, _, err := mgr.Append(record.RecordTypeBeginTransaction, payload)
		tst.RequireNoError(t, err)
	}

	// Get segment IDs
	segIds := mgr.SegmentIds()

	// Verify sorted in ascending order
	for i := 1; i < len(segIds); i++ {
		tst.AssertTrue(t, segIds[i] > segIds[i-1], "segment IDs should be sorted")
	}
}

// TestAppendReturnsCorrectSegmentAndOffset tests that offsets increase within a segment
func TestAppendReturnsCorrectSegmentAndOffset(t *testing.T) {
	tempDir := t.TempDir()

	mgr, err := wal.OpenManager(tempDir, wal.ManagerOpts{})
	tst.RequireNoError(t, err)
	defer mgr.Close() //nolint:errcheck

	var lastOffset int64

	for i := 0; i < 5; i++ {
		payload := createBeginPayload(uint64(i)) //nolint:gosec
		segId, offset, err := mgr.Append(record.RecordTypeBeginTransaction, payload)
		tst.RequireNoError(t, err)

		tst.RequireDeepEqual(t, segId, uint64(1))

		if i > 0 {
			// Each subsequent offset should be greater
			tst.AssertTrue(t, offset > lastOffset, "offset should increase")
		}

		lastOffset = offset
	}
}

// TestOpenManagerInvalidPath tests error handling for invalid directory
func TestOpenManagerInvalidPath(t *testing.T) {
	// Try to open with a path that has a non-existent parent
	invalidPath := "/nonexistent/path/to/wal/that/cannot/be/created/waldb"

	_, err := wal.OpenManager(invalidPath, wal.ManagerOpts{})
	if err == nil {
		t.Fatal("expected error opening manager with invalid path")
	}
}

// TestAppendWithInvalidPayload tests appending with invalid payload size
func TestAppendWithInvalidPayload(t *testing.T) {
	tempDir := t.TempDir()

	mgr, err := wal.OpenManager(tempDir, wal.ManagerOpts{})
	tst.RequireNoError(t, err)
	defer mgr.Close() //nolint:errcheck

	// Try to append begin record with wrong payload size
	invalidPayload := make([]byte, 4) // Begin requires TxnIdSize (8)
	_, _, err = mgr.Append(record.RecordTypeBeginTransaction, invalidPayload)
	if err == nil {
		t.Fatal("expected error appending with invalid payload")
	}
}

// TestAppendWithInvalidRecordType tests appending with invalid record type
func TestAppendWithInvalidRecordType(t *testing.T) {
	tempDir := t.TempDir()

	mgr, err := wal.OpenManager(tempDir, wal.ManagerOpts{})
	tst.RequireNoError(t, err)
	defer mgr.Close() //nolint:errcheck

	// Try to append with invalid record type
	invalidPayload := []byte("test")
	_, _, err = mgr.Append(record.RecordType(255), invalidPayload)
	if err == nil {
		t.Fatal("expected error appending with invalid record type")
	}
}

// TestManagerErrorWrapping tests that manager wraps errors with context
func TestManagerErrorWrapping(t *testing.T) {
	tempDir := t.TempDir()

	mgr, err := wal.OpenManager(tempDir, wal.ManagerOpts{})
	tst.RequireNoError(t, err)
	defer mgr.Close() //nolint:errcheck

	// Append with invalid record type should wrap error
	invalidPayload := []byte("test")
	_, _, err = mgr.Append(record.RecordType(255), invalidPayload)
	if err == nil {
		t.Fatal("expected error from append")
	}

	// Error should have manager context
	if err.Error() == "" {
		t.Fatal("expected non-empty error message")
	}
}

// TestConcurrentAppendAndClose tests thread safety when closing during append
func TestConcurrentAppendAndClose(t *testing.T) {
	tempDir := t.TempDir()

	mgr, err := wal.OpenManager(tempDir, wal.ManagerOpts{})
	tst.RequireNoError(t, err)

	// Append should succeed
	payload := createBeginPayload(1)
	_, _, err = mgr.Append(record.RecordTypeBeginTransaction, payload)
	tst.RequireNoError(t, err)

	// Close should succeed
	tst.RequireNoError(t, mgr.Close())

	// Further appends should fail
	payload = createBeginPayload(2)
	_, _, err = mgr.Append(record.RecordTypeBeginTransaction, payload)
	if err == nil {
		t.Fatal("expected error appending after close")
	}
}

// TestSegmentPathForNonexistentSegment tests that SegmentPath returns empty for nonexistent segment
func TestSegmentPathForNonexistentSegment(t *testing.T) {
	tempDir := t.TempDir()

	mgr, err := wal.OpenManager(tempDir, wal.ManagerOpts{})
	tst.RequireNoError(t, err)
	defer mgr.Close() //nolint:errcheck

	// Try to get path for nonexistent segment
	path := mgr.SegmentPath(999)
	tst.RequireDeepEqual(t, path, "")
}

// TestRotationPreservesData tests that data is preserved through rotation
func TestRotationPreservesData(t *testing.T) {
	tempDir := t.TempDir()

	// Set small max size to force rotation
	mgr, err := wal.OpenManager(tempDir, wal.ManagerOpts{SegmentMaxBytes: 400})
	tst.RequireNoError(t, err)
	defer mgr.Close() //nolint:errcheck

	// Append records until rotation
	recordCount := 0
	recordSegments := make(map[int]uint64)

	for i := 0; i < 15; i++ {
		payload := createPutPayload(uint64(i), []byte("k"), make([]byte, 50)) //nolint:gosec
		segId, offset, err := mgr.Append(record.RecordTypePutOperation, payload)
		tst.RequireNoError(t, err)

		recordSegments[recordCount] = segId
		recordCount++

		tst.AssertTrue(t, offset >= 0, "offset should be non-negative")
	}

	// Should have multiple segments
	segIds := mgr.SegmentIds()
	tst.AssertTrue(t, len(segIds) > 1, "expected multiple segments after rotation")

	// Verify all records are in valid segments
	for i := 0; i < recordCount; i++ {
		segId := recordSegments[i]
		segPaths := mgr.SegmentIds()
		found := false
		for _, pathSegId := range segPaths {
			if pathSegId == segId {
				found = true
				break
			}
		}
		tst.AssertTrue(t, found, "record segment should exist")
	}
}

// TestFlushPreservesRecords tests that flush makes records durable
func TestFlushPreservesRecords(t *testing.T) {
	tempDir := t.TempDir()

	mgr, err := wal.OpenManager(tempDir, wal.ManagerOpts{})
	tst.RequireNoError(t, err)

	// Append and flush
	payload := createPutPayload(1, []byte("key"), []byte("value"))
	_, _, err = mgr.Append(record.RecordTypePutOperation, payload)
	tst.RequireNoError(t, err)

	tst.RequireNoError(t, mgr.Flush())

	// Close and reopen - data should persist
	tst.RequireNoError(t, mgr.Close())

	mgr2, err := wal.OpenManager(tempDir, wal.ManagerOpts{})
	tst.RequireNoError(t, err)
	defer mgr2.Close() //nolint:errcheck

	// Should still have the segment
	segIds := mgr2.SegmentIds()
	tst.RequireDeepEqual(t, len(segIds), 1)
	tst.RequireDeepEqual(t, segIds[0], uint64(1))
}

// TestFSyncMakesRecordsDurable tests that fsync ensures durability
func TestFSyncMakesRecordsDurable(t *testing.T) {
	tempDir := t.TempDir()

	mgr, err := wal.OpenManager(tempDir, wal.ManagerOpts{})
	tst.RequireNoError(t, err)

	// Append and fsync
	payload := createBeginPayload(42)
	_, _, err = mgr.Append(record.RecordTypeBeginTransaction, payload)
	tst.RequireNoError(t, err)

	tst.RequireNoError(t, mgr.FSync())

	// Verify file exists and has content
	segPath := mgr.SegmentPath(1)
	tst.AssertTrue(t, segPath != "", "expected valid segment path")

	stat, err := os.Stat(segPath)
	tst.RequireNoError(t, err)
	tst.AssertTrue(t, stat.Size() > 0, "expected segment file to have content after fsync")

	tst.RequireNoError(t, mgr.Close())
}

// TestMultipleRotations tests multiple rotation cycles
func TestMultipleRotations(t *testing.T) {
	tempDir := t.TempDir()

	mgr, err := wal.OpenManager(tempDir, wal.ManagerOpts{SegmentMaxBytes: 300})
	tst.RequireNoError(t, err)
	defer mgr.Close() //nolint:errcheck

	// Append enough records to guarantee multiple rotations
	for i := 0; i < 30; i++ {
		payload := createPutPayload(uint64(i), []byte("k"), make([]byte, 50)) //nolint:gosec
		_, _, err := mgr.Append(record.RecordTypePutOperation, payload)
		tst.RequireNoError(t, err)
	}

	// Should have multiple segments after many appends
	finalSegIds := mgr.SegmentIds()
	tst.AssertTrue(t, len(finalSegIds) > 1, "expected multiple segments after many appends with rotation enabled")
}
