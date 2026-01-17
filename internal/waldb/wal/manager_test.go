package wal_test

import (
	"os"
	"path/filepath"
	"testing"

	tst "github.com/julianstephens/go-utils/tests"
	"github.com/julianstephens/waldb/internal/waldb/wal"
)

// TestOpenManagerCreatesDirectory tests that OpenManager creates a WAL directory if it doesn't exist
func TestOpenManagerCreatesDirectory(t *testing.T) {
	tempDir := t.TempDir()
	walDir := filepath.Join(tempDir, "new_wal_dir")

	provider, err := wal.OpenManager(walDir, wal.ManagerOpts{})
	tst.RequireNoError(t, err)
	tst.AssertNotNil(t, provider, "expected non-nil provider")

	// Verify directory was created
	_, err = os.Stat(walDir)
	tst.RequireNoError(t, err)
}

// TestOpenManagerCreatesFirstSegment tests that OpenManager creates the first segment
func TestOpenManagerCreatesFirstSegment(t *testing.T) {
	tempDir := t.TempDir()

	provider, err := wal.OpenManager(tempDir, wal.ManagerOpts{})
	tst.RequireNoError(t, err)

	// Should have exactly one segment
	segIds := provider.SegmentIDs()
	tst.RequireDeepEqual(t, len(segIds), 1)
	tst.RequireDeepEqual(t, segIds[0], uint64(1))
}

// TestOpenManagerDiscoverExistingSegments tests that OpenManager discovers existing segments
func TestOpenManagerDiscoverExistingSegments(t *testing.T) {
	tempDir := t.TempDir()

	// Create first provider
	_, err := wal.OpenManager(tempDir, wal.ManagerOpts{})
	tst.RequireNoError(t, err)

	// Open again and verify segments are discovered
	provider2, err := wal.OpenManager(tempDir, wal.ManagerOpts{})
	tst.RequireNoError(t, err)

	segIds := provider2.SegmentIDs()
	tst.RequireDeepEqual(t, len(segIds), 1)
	tst.RequireDeepEqual(t, segIds[0], uint64(1))
}

// TestSegmentIDs tests SegmentIDs returns segment IDs in sorted order
func TestSegmentIDs(t *testing.T) {
	tempDir := t.TempDir()

	provider, err := wal.OpenManager(tempDir, wal.ManagerOpts{})
	tst.RequireNoError(t, err)

	segIds := provider.SegmentIDs()
	tst.RequireDeepEqual(t, len(segIds), 1)
	tst.RequireDeepEqual(t, segIds[0], uint64(1))
}

// TestOpenSegmentExistingSegment tests opening an existing segment
func TestOpenSegmentExistingSegment(t *testing.T) {
	tempDir := t.TempDir()

	provider, err := wal.OpenManager(tempDir, wal.ManagerOpts{})
	tst.RequireNoError(t, err)

	// Open the existing segment 1
	reader, err := provider.OpenSegment(1)
	tst.RequireNoError(t, err)
	tst.AssertNotNil(t, reader, "expected non-nil reader")

	// Verify reader has correct segment ID
	tst.RequireDeepEqual(t, reader.SegID(), uint64(1))
}

// TestOpenSegmentNonExistent tests opening a non-existent segment
func TestOpenSegmentNonExistent(t *testing.T) {
	tempDir := t.TempDir()

	provider, err := wal.OpenManager(tempDir, wal.ManagerOpts{})
	tst.RequireNoError(t, err)

	// Try to open non-existent segment
	_, err = provider.OpenSegment(999)
	tst.AssertTrue(t, err != nil, "expected error opening non-existent segment")
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

// TestMultipleOpenManagerInstances tests opening multiple managers on same directory
func TestMultipleOpenManagerInstances(t *testing.T) {
	tempDir := t.TempDir()

	// Open first manager
	provider1, err := wal.OpenManager(tempDir, wal.ManagerOpts{})
	tst.RequireNoError(t, err)

	segIds1 := provider1.SegmentIDs()
	tst.RequireDeepEqual(t, len(segIds1), 1)

	// Open second manager on same directory
	provider2, err := wal.OpenManager(tempDir, wal.ManagerOpts{})
	tst.RequireNoError(t, err)

	segIds2 := provider2.SegmentIDs()
	tst.RequireDeepEqual(t, len(segIds2), 1)

	// Both should have the same segment ID
	tst.RequireDeepEqual(t, segIds1[0], segIds2[0])
}

// TestSegmentReaderSeekTo tests seeking within a segment
func TestSegmentReaderSeekTo(t *testing.T) {
	tempDir := t.TempDir()

	provider, err := wal.OpenManager(tempDir, wal.ManagerOpts{})
	tst.RequireNoError(t, err)

	reader, err := provider.OpenSegment(1)
	tst.RequireNoError(t, err)

	// Seek to offset 0 should succeed
	err = reader.SeekTo(0)
	tst.RequireNoError(t, err)
}

// TestSegmentProviderPersists tests that segment data persists across opens
func TestSegmentProviderPersists(t *testing.T) {
	tempDir := t.TempDir()

	// Open first provider
	provider1, err := wal.OpenManager(tempDir, wal.ManagerOpts{})
	tst.RequireNoError(t, err)

	segIds1 := provider1.SegmentIDs()
	initialSegId := segIds1[0]

	// Open second provider on same directory
	provider2, err := wal.OpenManager(tempDir, wal.ManagerOpts{})
	tst.RequireNoError(t, err)

	segIds2 := provider2.SegmentIDs()

	// Should discover the same segment
	tst.RequireDeepEqual(t, len(segIds2), len(segIds1))
	tst.RequireDeepEqual(t, segIds2[0], initialSegId)
}
