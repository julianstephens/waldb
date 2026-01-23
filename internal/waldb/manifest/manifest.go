package manifest

import (
	"fmt"
	"io/fs"
	"os"
	"path"
	"path/filepath"

	"github.com/julianstephens/go-utils/helpers"
	"github.com/julianstephens/go-utils/jsonutil"
	"github.com/julianstephens/waldb/internal/waldb"
	"github.com/julianstephens/waldb/internal/waldb/wal/record"
)

const ManifestFileName = "MANIFEST.json"

// Manifest represents the database manifest file structure
type Manifest struct {
	Version           int  `json:"version"`
	FsyncOnCommit     bool `json:"fsync_on_commit"`
	MaxKeySize        int  `json:"max_key_size"`
	MaxValueSize      int  `json:"max_value_size"`
	WalSegmentMaxSize int  `json:"wal_segment_max_size"`
	WalSegmentNextID  *int `json:"wal_segment_next_id,omitempty"`
	WalLogMaxSize     *int `json:"wal_log_max_size,omitempty"`
	WalLogMaxBackups  *int `json:"wal_log_max_backups,omitempty"`
}

// DefaultManifest returns a Manifest with default settings
func DefaultManifest() *Manifest {
	return &Manifest{
		Version:           waldb.ManifestVersion,
		FsyncOnCommit:     true,
		MaxKeySize:        record.MaxKeySize,
		MaxValueSize:      record.MaxValueSize,
		WalSegmentMaxSize: int(waldb.DefaultSegmentMaxBytes),
		WalSegmentNextID:  nil,
		WalLogMaxSize:     nil,
		WalLogMaxBackups:  nil,
	}
}

// CreateDefaultManifest creates a manifest file with default settings
func CreateDefaultManifest() error {
	manifestPath, err := getManifestPath()
	if err != nil {
		return err
	}

	if exists := helpers.Exists(manifestPath); exists {
		return &ManifestError{
			Kind: ManifestErrorKindAlreadyExists,
			Err:  fmt.Errorf("manifest already exists at %s", manifestPath),
		}
	}

	m := DefaultManifest()
	data, err := jsonutil.Marshal(m)
	if err != nil {
		return &ManifestError{Kind: ManifestErrorKindEncode, Err: err}
	}

	return writeFile(manifestPath, data)
}

// Open reads the manifest from disk
func Open() (*Manifest, error) {
	manifestPath, err := getManifestPath()
	if err != nil {
		return nil, err
	}

	if exists := helpers.Exists(manifestPath); !exists {
		return nil, &ManifestError{Kind: ManifestErrorKindNotFound, Err: fs.ErrNotExist}
	}

	m := &Manifest{}
	if err := jsonutil.ReadFileStrict(manifestPath, m); err != nil {
		return nil, &ManifestError{Kind: ManifestErrorKindDecode, Err: err}
	}

	if m.Version > waldb.ManifestVersion {
		return nil, &ManifestError{
			Kind: ManifestErrorKindUnsupportedVersion,
			Err:  fmt.Errorf("manifest version %d is not supported", m.Version),
		}
	}

	return m, nil
}

// Save writes the manifest to disk
func (m *Manifest) Save() error {
	manifestPath, err := getManifestPath()
	if err != nil {
		return err
	}

	if exists := helpers.Exists(manifestPath); !exists {
		return &ManifestError{Kind: ManifestErrorKindNotFound, Err: fs.ErrNotExist}
	}

	data, err := jsonutil.Marshal(m)
	if err != nil {
		return &ManifestError{Kind: ManifestErrorKindEncode, Err: err}
	}

	return writeFile(manifestPath, data)
}

func getManifestPath() (manifestPath string, err error) {
	wd, err := os.Getwd()
	if err != nil {
		err = &ManifestError{Kind: ManifestErrorKindWorkingDirectory, Err: err}
		return
	}
	manifestPath = path.Join(wd, ManifestFileName)
	return
}

func writeFile(filePath string, data []byte) error {
	if err := helpers.AtomicFileWrite(filePath, data); err != nil {
		return &ManifestError{Kind: ManifestErrorKindWrite, Err: err}
	}
	f, err := os.Open(filepath.Dir(filePath)) //nolint:gosec
	if err != nil {
		return &ManifestError{Kind: ManifestErrorKindWrite, Err: err}
	}
	defer func() { _ = f.Close() }()

	if err := f.Sync(); err != nil {
		return &ManifestError{Kind: ManifestErrorKindWrite, Err: err}
	}
	return nil
}
