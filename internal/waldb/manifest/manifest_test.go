package manifest

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// TestInit verifies manifest creation with atomic write
func TestInit(t *testing.T) {
	tmpDir := t.TempDir()

	// Create default manifest
	_, err := Init(tmpDir)
	if err != nil {
		t.Fatalf("Init() error = %v", err)
	}

	// Verify manifest is readable and valid
	opened, err := Open(tmpDir)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}

	if opened == nil {
		t.Fatal("expected manifest, got nil")
	}
	if opened.FormatVersion != 1 {
		t.Errorf("expected Version=1, got %d", opened.FormatVersion)
	}
	if !opened.FsyncOnCommit {
		t.Errorf("expected FsyncOnCommit=true, got false")
	}
}

// TestInit_AlreadyExists validates error when manifest exists
func TestInit_AlreadyExists(t *testing.T) {
	tmpDir := t.TempDir()

	// Create first manifest
	_, err := Init(tmpDir)
	if err != nil {
		t.Fatalf("first Init() error = %v", err)
	}

	// Attempt to create again
	_, err = Init(tmpDir)
	if err == nil {
		t.Fatalf("expected error when manifest already exists, got nil")
	}

	// Verify it's a ManifestError with AlreadyExists kind
	manifestErr, ok := err.(*ManifestError)
	if !ok {
		t.Fatalf("expected ManifestError, got %T", err)
	}
	if manifestErr.Kind != ManifestErrorKindAlreadyExists {
		t.Errorf("expected Kind=AlreadyExists, got %v", manifestErr.Kind)
	}
}

// TestSave_AtomicWrite verifies that Save() performs atomic writes with valid JSON
func TestSave_AtomicWrite(t *testing.T) {
	tmpDir := t.TempDir()

	// Create initial manifest
	_, err := Init(tmpDir)
	if err != nil {
		t.Fatalf("Init() error = %v", err)
	}

	// Open the manifest and save it
	manifest, err := Open(tmpDir)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}

	if err := manifest.Save(tmpDir); err != nil {
		t.Fatalf("Save() error = %v", err)
	}

	// Read the saved manifest file directly
	data, err := os.ReadFile(filepath.Join(tmpDir, ManifestFileName)) //nolint:gosec
	if err != nil {
		t.Fatalf("failed to read manifest file: %v", err)
	}

	// Verify it's valid JSON and matches what was saved
	var saved Manifest
	if err := json.Unmarshal(data, &saved); err != nil {
		t.Fatalf("manifest file is not valid JSON: %v", err)
	}

	if saved.FormatVersion != manifest.FormatVersion {
		t.Errorf("saved Version=%d, expected %d", saved.FormatVersion, manifest.FormatVersion)
	}
	if saved.FsyncOnCommit != manifest.FsyncOnCommit {
		t.Errorf("saved FsyncOnCommit=%v, expected %v", saved.FsyncOnCommit, manifest.FsyncOnCommit)
	}
}

// TestSave_MultipleTimes verifies that Save() can be called multiple times atomically
func TestSave_MultipleTimes(t *testing.T) {
	tmpDir := t.TempDir()

	// Create initial manifest
	_, err := Init(tmpDir)
	if err != nil {
		t.Fatalf("Init() error = %v", err)
	}

	// Open and save first time
	manifest1, err := Open(tmpDir)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if err := manifest1.Save(tmpDir); err != nil {
		t.Fatalf("first Save() error = %v", err)
	}

	// Modify and save second time
	manifest1.FsyncOnCommit = false
	segID := 10
	manifest1.WalNextSegmentID = &segID
	if err := manifest1.Save(tmpDir); err != nil {
		t.Fatalf("second Save() error = %v", err)
	}

	// Open and verify the second save took effect
	opened, err := Open(tmpDir)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}

	if opened.FsyncOnCommit {
		t.Errorf("expected FsyncOnCommit=false after second save, got true")
	}
	if opened.WalNextSegmentID == nil || *opened.WalNextSegmentID != 10 {
		t.Errorf("expected WalNextSegmentID=10, got %v", opened.WalNextSegmentID)
	}
}

// TestOpen_FileNotFound validates error when manifest doesn't exist
func TestOpen_FileNotFound(t *testing.T) {
	tmpDir := t.TempDir()

	manifest, err := Open(tmpDir)
	if err == nil {
		t.Fatalf("expected error when manifest doesn't exist, got nil")
	}

	if manifest != nil {
		t.Errorf("expected nil manifest, got %v", manifest)
	}

	// Verify it's a ManifestError with NotFound kind
	manifestErr, ok := err.(*ManifestError)
	if !ok {
		t.Fatalf("expected ManifestError, got %T", err)
	}
	if manifestErr.Kind != ManifestErrorKindNotFound {
		t.Errorf("expected Kind=NotFound, got %v", manifestErr.Kind)
	}
}

// TestOpen_InvalidJSON validates error handling for corrupt manifest
func TestOpen_InvalidJSON(t *testing.T) {
	tmpDir := t.TempDir()

	// Write invalid JSON with secure permissions
	manifestPath := filepath.Join(tmpDir, ManifestFileName)
	if err := os.WriteFile(manifestPath, []byte("{invalid json}"), 0o600); err != nil {
		t.Fatalf("failed to write invalid manifest: %v", err)
	}

	_, err := Open(tmpDir)
	if err == nil {
		t.Fatalf("expected error for invalid JSON, got nil")
	}

	// Note: Open may still return a partially constructed Manifest
	// The important thing is that it returns an error

	// Verify it's a ManifestError with Decode kind
	manifestErr, ok := err.(*ManifestError)
	if !ok {
		t.Fatalf("expected ManifestError, got %T", err)
	}
	if manifestErr.Kind != ManifestErrorKindDecode {
		t.Errorf("expected Kind=Decode, got %v", manifestErr.Kind)
	}
}

// TestOpen_UnsupportedVersion validates version checking
func TestOpen_UnsupportedVersion(t *testing.T) {
	tmpDir := t.TempDir()

	// Create a manifest with unsupported version
	futureVersion := 999
	manifestData := fmt.Sprintf(
		`{"format_version":%d,"fsync_on_commit":true,"max_key_bytes":4096,"max_value_bytes":4194304,"wal_segment_max_bytes":268435456}`,
		futureVersion,
	)
	manifestPath := filepath.Join(tmpDir, ManifestFileName)
	if err := os.WriteFile(manifestPath, []byte(manifestData), 0o600); err != nil {
		t.Fatalf("failed to write manifest: %v", err)
	}

	_, err := Open(tmpDir)
	if err == nil {
		t.Fatalf("expected error for unsupported version, got nil")
	}

	// Note: Open may still return a partially constructed Manifest
	// The important thing is that it returns an error

	// Verify it's a ManifestError with UnsupportedVersion kind
	manifestErr, ok := err.(*ManifestError)
	if !ok {
		t.Fatalf("expected ManifestError, got %T", err)
	}
	if manifestErr.Kind != ManifestErrorKindUnsupportedVersion {
		t.Errorf("expected Kind=UnsupportedVersion, got %v", manifestErr.Kind)
	}
}

// TestValidate_Valid tests that a valid manifest passes validation
func TestValidate_Valid(t *testing.T) {
	m := defaultManifest()
	if err := m.Validate(); err != nil {
		t.Errorf("expected valid manifest to pass validation, got error: %v", err)
	}
}

// TestValidate_InvalidFormatVersion tests that non-positive format version is rejected
func TestValidate_InvalidFormatVersion(t *testing.T) {
	tests := []struct {
		name    string
		version int
	}{
		{"ZeroVersion", 0},
		{"NegativeVersion", -1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := defaultManifest()
			m.FormatVersion = tt.version

			err := m.Validate()
			if err == nil {
				t.Fatalf("expected error for version %d, got nil", tt.version)
			}

			manifestErr, ok := err.(*ManifestError)
			if !ok {
				t.Fatalf("expected ManifestError, got %T", err)
			}
			if manifestErr.Kind != ManifestErrorKindCorrupted {
				t.Errorf("expected Kind=Corrupted, got %v", manifestErr.Kind)
			}
		})
	}
}

// TestValidate_InvalidMaxKeyBytes tests max key bytes validation
func TestValidate_InvalidMaxKeyBytes(t *testing.T) {
	tests := []struct {
		name        string
		maxKeyBytes int
		expectErr   bool
	}{
		{"ZeroMaxKeyBytes", 0, true},
		{"NegativeMaxKeyBytes", -1, true},
		{"MaxKeySizeBoundary", 4096, false},
		{"ExceedsMaxKeySize", 4097, true},
		{"LargeExceedance", 10000, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := defaultManifest()
			m.MaxKeyBytes = tt.maxKeyBytes

			err := m.Validate()
			if tt.expectErr && err == nil {
				t.Fatalf("expected error for MaxKeyBytes=%d, got nil", tt.maxKeyBytes)
			}
			if !tt.expectErr && err != nil {
				t.Fatalf("expected no error for MaxKeyBytes=%d, got: %v", tt.maxKeyBytes, err)
			}

			if err != nil {
				manifestErr, ok := err.(*ManifestError)
				if !ok {
					t.Fatalf("expected ManifestError, got %T", err)
				}
				if manifestErr.Kind != ManifestErrorKindCorrupted {
					t.Errorf("expected Kind=Corrupted, got %v", manifestErr.Kind)
				}
			}
		})
	}
}

// TestValidate_InvalidMaxValueBytes tests max value bytes validation
func TestValidate_InvalidMaxValueBytes(t *testing.T) {
	tests := []struct {
		name          string
		maxValueBytes int
		expectErr     bool
	}{
		{"ZeroMaxValueBytes", 0, true},
		{"NegativeMaxValueBytes", -1, true},
		{"MaxValueSizeBoundary", 4194304, false},
		{"ExceedsMaxValueSize", 4194305, true},
		{"LargeExceedance", 5000000, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := defaultManifest()
			m.MaxValueBytes = tt.maxValueBytes

			err := m.Validate()
			if tt.expectErr && err == nil {
				t.Fatalf("expected error for MaxValueBytes=%d, got nil", tt.maxValueBytes)
			}
			if !tt.expectErr && err != nil {
				t.Fatalf("expected no error for MaxValueBytes=%d, got: %v", tt.maxValueBytes, err)
			}

			if err != nil {
				manifestErr, ok := err.(*ManifestError)
				if !ok {
					t.Fatalf("expected ManifestError, got %T", err)
				}
				if manifestErr.Kind != ManifestErrorKindCorrupted {
					t.Errorf("expected Kind=Corrupted, got %v", manifestErr.Kind)
				}
			}
		})
	}
}

// TestValidate_InvalidWalSegmentMaxBytes tests WAL segment max bytes validation
func TestValidate_InvalidWalSegmentMaxBytes(t *testing.T) {
	tests := []struct {
		name               string
		walSegmentMaxBytes int
		expectErr          bool
	}{
		{"ZeroWalSegmentMaxBytes", 0, true},
		{"NegativeWalSegmentMaxBytes", -1, true},
		{"TooSmall_BelowMinRecordFrameSize", 8, true}, // MinRecordFrameSize = 9
		{"MinRecordFrameSizeBoundary", 9, false},      // MinRecordFrameSize = 9, this is exactly at boundary
		{"ValidLargeValue", 268435456, false},         // 256 MB default
		{"SmallValidValue", 1024, false},              // Well above MinRecordFrameSize
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := defaultManifest()
			m.WalSegmentMaxBytes = tt.walSegmentMaxBytes

			err := m.Validate()
			if tt.expectErr && err == nil {
				t.Fatalf("expected error for WalSegmentMaxBytes=%d, got nil", tt.walSegmentMaxBytes)
			}
			if !tt.expectErr && err != nil {
				t.Fatalf("expected no error for WalSegmentMaxBytes=%d, got: %v", tt.walSegmentMaxBytes, err)
			}

			if err != nil {
				manifestErr, ok := err.(*ManifestError)
				if !ok {
					t.Fatalf("expected ManifestError, got %T", err)
				}
				if manifestErr.Kind != ManifestErrorKindCorrupted {
					t.Errorf("expected Kind=Corrupted, got %v", manifestErr.Kind)
				}
			}
		})
	}
}

// TestValidate_MultipleInvalidFields tests validation with multiple invalid fields
func TestValidate_MultipleInvalidFields(t *testing.T) {
	m := &Manifest{
		FormatVersion:      0, // Invalid
		FsyncOnCommit:      true,
		MaxKeyBytes:        -1, // Invalid
		MaxValueBytes:      4194304,
		WalSegmentMaxBytes: 268435456,
	}

	err := m.Validate()
	if err == nil {
		t.Fatalf("expected error for invalid manifest with multiple fields, got nil")
	}

	// Validate should catch at least the first invalid field
	manifestErr, ok := err.(*ManifestError)
	if !ok {
		t.Fatalf("expected ManifestError, got %T", err)
	}
	if manifestErr.Kind != ManifestErrorKindCorrupted {
		t.Errorf("expected Kind=Corrupted, got %v", manifestErr.Kind)
	}
}

// TestValidate_BoundaryValues tests validation at boundaries
func TestValidate_BoundaryValues(t *testing.T) {
	m := &Manifest{
		FormatVersion:      1, // Minimum valid positive value
		FsyncOnCommit:      true,
		MaxKeyBytes:        1, // Minimum positive value
		MaxValueBytes:      1, // Minimum positive value
		WalSegmentMaxBytes: 268435456,
	}

	err := m.Validate()
	if err != nil {
		t.Errorf("expected valid manifest with minimum positive values, got error: %v", err)
	}
}
