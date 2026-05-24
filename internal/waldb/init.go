package waldb

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/julianstephens/go-utils/helpers"

	"github.com/julianstephens/waldb/internal/logger"
)

var (
	ErrInitFailed    = errors.New("waldb: init failed")
	ErrInvalidDir    = errors.New("waldb: invalid dir")
	ErrAlreadyExists = errors.New("waldb: already exists")
)

// manifestInitFn is the function used to initialize the manifest.
// It is set via RegisterManifestInit (called by the manifest package init()).
// It is a variable so it can be replaced in tests.
var manifestInitFn func(string) error

// defaultManifestInitFn stores the registered default so tests can restore it.
var defaultManifestInitFn func(string) error

// RegisterManifestInit sets the manifest initialization function.
// It is called automatically by the waldb/manifest package init().
func RegisterManifestInit(fn func(string) error) {
	manifestInitFn = fn
	defaultManifestInitFn = fn
}

// Init initializes a new WAL database at the given directory path.
// It creates the necessary directory structure and manifest file.
// If the directory already exists and is not empty, it returns an error.
//
// The caller must import waldb/manifest (directly or transitively) to ensure
// the manifest initializer is registered before calling Init.
func Init(dir string, lg logger.Logger) error {
	if lg == nil {
		lg = &logger.NoOpLogger{}
	}

	if manifestInitFn == nil {
		return fmt.Errorf(
			"init %s: %w: manifest initializer not registered (import waldb/manifest)",
			dir,
			ErrInitFailed,
		)
	}

	exists, info, err := helpers.ExistsWithInfo(dir)
	if err != nil {
		lg.Error("failed to check if directory exists", err, "dir", dir)
		return fmt.Errorf("init %s: %w: %v", dir, ErrInitFailed, err)
	}
	if exists {
		if !info.IsDir() {
			lg.Error("provided path is not a directory", nil, "dir", dir)
			return fmt.Errorf("init %s: %w: path exists but is not a directory", dir, ErrInvalidDir)
		}

		entries, err := os.ReadDir(dir)
		if err != nil {
			lg.Error("failed to read existing directory", err, "dir", dir)
			return fmt.Errorf("init %s: %w: %v", dir, ErrInitFailed, err)
		}
		if len(entries) > 0 {
			lg.Error("cannot initialize database: directory is not empty", nil, "dir", dir, "entry_count", len(entries))
			return fmt.Errorf("init %s: %w: directory is not empty", dir, ErrAlreadyExists)
		}
	}

	lg.Info("initializing new database", "dir", dir)

	lg.Debug("ensuring database directory structure", "dir", dir)
	if err := helpers.Ensure(dir, true); err != nil {
		lg.Error("failed to ensure database directory", err, "dir", dir)
		return fmt.Errorf("init %s: %w: %v", dir, ErrInitFailed, err)
	}

	created := []string{}
	lg.Debug("creating WAL directory", "dir", dir)
	if err := os.Mkdir(filepath.Join(dir, WALDirName), 0750); err != nil {
		lg.Error("failed to create WAL directory", err, "dir", dir)
		return fmt.Errorf("init %s: %w: %v", dir, ErrInitFailed, err)
	}
	created = append(created, WALDirName)

	lg.Debug("creating lock file", "dir", dir)
	if file, err := os.Create(filepath.Join(dir, LockFileName)); err != nil { // nolint:gosec
		lg.Error("failed to create lock file", err, "dir", dir)
		return fmt.Errorf("init %s: %w: %v", dir, ErrInitFailed, err)
	} else {
		if err := file.Close(); err != nil {
			lg.Error("failed to close lock file", err, "dir", dir)
			return fmt.Errorf("init %s: %w: %v", dir, ErrInitFailed, err)
		}
	}
	created = append(created, LockFileName)

	lg.Debug("initializing manifest", "dir", dir)
	if err := manifestInitFn(dir); err != nil {
		lg.Error("failed to initialize manifest", err, "dir", dir)
		lg.Debug("cleaning up created files/directories", "dir", dir, "created_count", len(created))
		for _, name := range created {
			path := filepath.Join(dir, name)
			if err := os.RemoveAll(path); err != nil {
				lg.Error("failed to clean up path during init error handling", err, "path", path)
			} else {
				lg.Debug("cleaned up path during init error handling", "path", path)
			}
		}
		return fmt.Errorf("init %s: %w: %v", dir, ErrInitFailed, err)
	}

	lg.Info("database initialized successfully", "dir", dir)
	return nil
}
