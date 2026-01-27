package testutil

import (
	"os"
	"path/filepath"
	"testing"

	tst "github.com/julianstephens/go-utils/tests"
	"github.com/julianstephens/waldb/internal/waldb"
	"github.com/julianstephens/waldb/internal/waldb/manifest"
)

// SetupTestDB initializes a database directory with manifest, lock file, and WAL directory
func SetupTestDB(t *testing.T, dbPath string) {
	// Create parent directory
	tst.RequireNoError(t, os.MkdirAll(dbPath, 0o750))

	// Initialize manifest
	_, err := manifest.Init(dbPath)
	tst.RequireNoError(t, err)

	// Create LOCK file
	lockPath := filepath.Join(dbPath, waldb.LockFileName)
	lockFile, err := os.Create(lockPath) // nolint:gosec
	tst.RequireNoError(t, err)
	err = lockFile.Close()
	tst.RequireNoError(t, err)

	// Create WAL directory
	walDir := filepath.Join(dbPath, waldb.WALDirName)
	err = os.MkdirAll(walDir, 0o750)
	tst.RequireNoError(t, err)
}
