package waldb

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/julianstephens/go-utils/helpers"

	"github.com/julianstephens/waldb/internal/logger"
	"github.com/julianstephens/waldb/internal/waldb/config"
	"github.com/julianstephens/waldb/internal/waldb/manifest"
)

var (
	ErrInitFailed    = errors.New("waldb: init failed")
	ErrInvalidDir    = errors.New("waldb: invalid dir")
	ErrAlreadyExists = errors.New("waldb: already exists")
)

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
	if err := os.Mkdir(filepath.Join(dir, config.WALDirName), 0750); err != nil {
		lg.Error("failed to create WAL directory", err, "dir", dir)
		return fmt.Errorf("init %s: %w: %v", dir, ErrInitFailed, err)
	}
	created = append(created, config.WALDirName)

	lg.Debug("creating lock file", "dir", dir)
	if file, err := os.Create(filepath.Join(dir, config.LockFileName)); err != nil { // nolint:gosec
		lg.Error("failed to create lock file", err, "dir", dir)
		return fmt.Errorf("init %s: %w: %v", dir, ErrInitFailed, err)
	} else {
		if err := file.Close(); err != nil {
			lg.Error("failed to close lock file", err, "dir", dir)
			return fmt.Errorf("init %s: %w: %v", dir, ErrInitFailed, err)
		}
	}
	created = append(created, config.LockFileName)

	lg.Debug("initializing manifest", "dir", dir)
	if _, err := manifest.Init(dir); err != nil {
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
