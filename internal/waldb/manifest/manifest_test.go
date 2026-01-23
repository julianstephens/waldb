package manifest

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// TestCreateDefaultManifest verifies manifest creation with atomic write
func TestCreateDefaultManifest(t *testing.T) {
	tmpDir := t.TempDir()
	originalWd, err := os.Getwd()
	if err != nil {
		t.Fatalf("failed to get working directory: %v", err)
	}
	defer func() { _ = os.Chdir(originalWd) }()

	if err := os.Chdir(tmpDir); err != nil {
		t.Fatalf("failed to change to temp directory: %v", err)
	}

	// Create default manifest
	if err := CreateDefaultManifest(); err != nil {
		t.Fatalf("CreateDefaultManifest() error = %v", err)
	}

	// Verify manifest is readable and valid
	opened, err := Open()
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}

	if opened == nil {
		t.Fatal("expected manifest, got nil")
	}
	if opened.Version != 1 {
		t.Errorf("expected Version=1, got %d", opened.Version)
	}
	if !opened.FsyncOnCommit {
		t.Errorf("expected FsyncOnCommit=true, got false")
	}
}

// TestCreateDefaultManifest_AlreadyExists validates error when manifest exists
func TestCreateDefaultManifest_AlreadyExists(t *testing.T) {
	tmpDir := t.TempDir()
	originalWd, err := os.Getwd()
	if err != nil {
		t.Fatalf("failed to get working directory: %v", err)
	}
	defer func() { _ = os.Chdir(originalWd) }()

	if err := os.Chdir(tmpDir); err != nil {
		t.Fatalf("failed to change to temp directory: %v", err)
	}

	// Create first manifest
	if err := CreateDefaultManifest(); err != nil {
		t.Fatalf("first CreateDefaultManifest() error = %v", err)
	}

	// Attempt to create again
	err = CreateDefaultManifest()
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
	originalWd, err := os.Getwd()
	if err != nil {
		t.Fatalf("failed to get working directory: %v", err)
	}
	defer func() { _ = os.Chdir(originalWd) }()

	if err := os.Chdir(tmpDir); err != nil {
		t.Fatalf("failed to change to temp directory: %v", err)
	}

	// Create initial manifest
	if err := CreateDefaultManifest(); err != nil {
		t.Fatalf("CreateDefaultManifest() error = %v", err)
	}

	manifest := DefaultManifest()
	if err := manifest.Save(); err != nil {
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

	if saved.Version != manifest.Version {
		t.Errorf("saved Version=%d, expected %d", saved.Version, manifest.Version)
	}
	if saved.FsyncOnCommit != manifest.FsyncOnCommit {
		t.Errorf("saved FsyncOnCommit=%v, expected %v", saved.FsyncOnCommit, manifest.FsyncOnCommit)
	}
}

// TestSave_MultipleTimes verifies that Save() can be called multiple times atomically
func TestSave_MultipleTimes(t *testing.T) {
	tmpDir := t.TempDir()
	originalWd, err := os.Getwd()
	if err != nil {
		t.Fatalf("failed to get working directory: %v", err)
	}
	defer func() { _ = os.Chdir(originalWd) }()

	if err := os.Chdir(tmpDir); err != nil {
		t.Fatalf("failed to change to temp directory: %v", err)
	}

	// Create initial manifest
	if err := CreateDefaultManifest(); err != nil {
		t.Fatalf("CreateDefaultManifest() error = %v", err)
	}

	// Save first time
	manifest1 := DefaultManifest()
	if err := manifest1.Save(); err != nil {
		t.Fatalf("first Save() error = %v", err)
	}

	// Modify and save second time
	manifest2 := DefaultManifest()
	manifest2.FsyncOnCommit = false
	segID := 10
	manifest2.WalSegmentNextID = &segID
	if err := manifest2.Save(); err != nil {
		t.Fatalf("second Save() error = %v", err)
	}

	// Open and verify the second save took effect
	opened, err := Open()
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}

	if opened.FsyncOnCommit {
		t.Errorf("expected FsyncOnCommit=false after second save, got true")
	}
	if opened.WalSegmentNextID == nil || *opened.WalSegmentNextID != 10 {
		t.Errorf("expected WalSegmentNextID=10, got %v", opened.WalSegmentNextID)
	}
}

// TestOpen_FileNotFound validates error when manifest doesn't exist
func TestOpen_FileNotFound(t *testing.T) {
	tmpDir := t.TempDir()
	originalWd, err := os.Getwd()
	if err != nil {
		t.Fatalf("failed to get working directory: %v", err)
	}
	defer func() { _ = os.Chdir(originalWd) }()

	if err := os.Chdir(tmpDir); err != nil {
		t.Fatalf("failed to change to temp directory: %v", err)
	}

	manifest, err := Open()
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
	originalWd, err := os.Getwd()
	if err != nil {
		t.Fatalf("failed to get working directory: %v", err)
	}
	defer func() { _ = os.Chdir(originalWd) }()

	if err := os.Chdir(tmpDir); err != nil {
		t.Fatalf("failed to change to temp directory: %v", err)
	}

	// Write invalid JSON with secure permissions
	manifestPath := filepath.Join(tmpDir, ManifestFileName)
	if err := os.WriteFile(manifestPath, []byte("{invalid json}"), 0o600); err != nil {
		t.Fatalf("failed to write invalid manifest: %v", err)
	}

	manifest, err := Open()
	if err == nil {
		t.Fatalf("expected error for invalid JSON, got nil")
	}

	if manifest != nil {
		t.Errorf("expected nil manifest, got %v", manifest)
	}

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
	originalWd, err := os.Getwd()
	if err != nil {
		t.Fatalf("failed to get working directory: %v", err)
	}
	defer func() { _ = os.Chdir(originalWd) }()

	if err := os.Chdir(tmpDir); err != nil {
		t.Fatalf("failed to change to temp directory: %v", err)
	}

	// Create a manifest with unsupported version
	futureVersion := 999
	manifestData := fmt.Sprintf(
		`{"version":%d,"fsync_on_commit":true,"max_key_size":4096,"max_value_size":4194304,"wal_segment_max_size":268435456}`,
		futureVersion,
	)
	manifestPath := filepath.Join(tmpDir, ManifestFileName)
	if err := os.WriteFile(manifestPath, []byte(manifestData), 0o600); err != nil {
		t.Fatalf("failed to write manifest: %v", err)
	}

	manifest, err := Open()
	if err == nil {
		t.Fatalf("expected error for unsupported version, got nil")
	}

	if manifest != nil {
		t.Errorf("expected nil manifest, got %v", manifest)
	}

	// Verify it's a ManifestError with UnsupportedVersion kind
	manifestErr, ok := err.(*ManifestError)
	if !ok {
		t.Fatalf("expected ManifestError, got %T", err)
	}
	if manifestErr.Kind != ManifestErrorKindUnsupportedVersion {
		t.Errorf("expected Kind=UnsupportedVersion, got %v", manifestErr.Kind)
	}
}
