package manifest

import (
	"errors"
	"fmt"
	"io/fs"
	"path/filepath"

	"github.com/julianstephens/go-utils/helpers"
	"github.com/julianstephens/go-utils/jsonutil"

	"github.com/julianstephens/waldb/internal/waldb/config"
	"github.com/julianstephens/waldb/internal/waldb/wal/record"
)

// Manifest represents the database manifest file structure
type Manifest struct {
	FormatVersion      int     `json:"format_version"`
	FsyncOnCommit      bool    `json:"fsync_on_commit"`
	MaxKeyBytes        int     `json:"max_key_bytes"`
	MaxValueBytes      int     `json:"max_value_bytes"`
	WalSegmentMaxBytes int     `json:"wal_segment_max_bytes"`
	WalNextSegmentID   *int    `json:"wal_next_segment_id,omitempty"`
	WalLogMaxBytes     *int    `json:"wal_log_max_bytes,omitempty"`
	WalLogMaxBackups   *int    `json:"wal_log_max_backups,omitempty"`
	LogDir             *string `json:"log_dir,omitempty"`
	LogFileName        *string `json:"log_file_name,omitempty"`
	LogMaxSize         *int    `json:"log_max_size,omitempty"`
	LogMaxBackups      *int    `json:"log_max_backups,omitempty"`
}

// DefaultManifest returns a Manifest with default settings
func DefaultManifest() *Manifest {
	return &Manifest{
		FormatVersion:      config.ManifestVersion,
		FsyncOnCommit:      true,
		MaxKeyBytes:        record.MaxKeySize,
		MaxValueBytes:      record.MaxValueSize,
		WalSegmentMaxBytes: int(config.DefaultSegmentMaxBytes),
		WalNextSegmentID:   nil,
		WalLogMaxBytes:     nil,
		WalLogMaxBackups:   nil,
	}
}

// Init creates a new manifest file with default settings
func Init(dir string) (m *Manifest, err error) {
	manifestPath := filepath.Join(dir, config.ManifestFileName)

	if exists := helpers.Exists(manifestPath); exists {
		err = &ManifestError{
			Kind: ManifestErrorKindAlreadyExists,
			Err:  fmt.Errorf("manifest already exists at %s", manifestPath),
		}
		return
	}

	m = DefaultManifest()
	data, err2 := jsonutil.Marshal(m)
	if err2 != nil {
		err = &ManifestError{Kind: ManifestErrorKindEncode, Err: err2}
		return
	}

	if err2 := helpers.AtomicFileWrite(manifestPath, data); err2 != nil {
		err = &ManifestError{Kind: ManifestErrorKindWrite, Err: err2}
	}
	return
}

// Open reads the manifest from disk
func Open(dir string) (m *Manifest, err error) {
	manifestPath := filepath.Join(dir, config.ManifestFileName)

	if exists := helpers.Exists(manifestPath); !exists {
		err = &ManifestError{Kind: ManifestErrorKindNotFound, Err: fs.ErrNotExist}
		return
	}

	m = &Manifest{}
	if err2 := jsonutil.ReadFileStrict(manifestPath, m); err2 != nil {
		err = &ManifestError{Kind: ManifestErrorKindDecode, Err: err2}
		return
	}

	if m.FormatVersion > config.ManifestVersion {
		err = &ManifestError{
			Kind: ManifestErrorKindUnsupportedVersion,
			Err:  fmt.Errorf("manifest version %d is not supported", m.FormatVersion),
		}
		return
	}

	if err = m.Validate(); err != nil {
		return
	}

	return
}

// Save writes the manifest to disk
func (m *Manifest) Save(dir string) error {
	manifestPath := filepath.Join(dir, config.ManifestFileName)

	if exists := helpers.Exists(manifestPath); !exists {
		return &ManifestError{Kind: ManifestErrorKindNotFound, Err: fs.ErrNotExist}
	}

	data, err := jsonutil.Marshal(m)
	if err != nil {
		return &ManifestError{Kind: ManifestErrorKindEncode, Err: err}
	}

	if err := helpers.AtomicFileWrite(manifestPath, data); err != nil {
		return &ManifestError{Kind: ManifestErrorKindWrite, Err: err}
	}
	return nil
}

// Validate checks the manifest for valid settings
func (m *Manifest) Validate() error {
	if m.FormatVersion <= 0 {
		return mustBePositive("format_version")
	}
	if m.MaxKeyBytes <= 0 {
		return mustBePositive("max_key_bytes")
	}
	if m.MaxKeyBytes > record.MaxKeySize {
		return &ManifestError{
			Kind: ManifestErrorKindCorrupted,
			Err:  errors.New("max_key_bytes too large"),
		}
	}
	if m.MaxValueBytes <= 0 {
		return mustBePositive("max_value_bytes")
	}
	if m.MaxValueBytes > record.MaxValueSize {
		return &ManifestError{
			Kind: ManifestErrorKindCorrupted,
			Err:  errors.New("max_value_bytes too large"),
		}

	}
	if m.WalSegmentMaxBytes <= 0 {
		return mustBePositive("wal_segment_max_bytes")
	}
	if m.WalSegmentMaxBytes < record.MinRecordFrameSize {
		return &ManifestError{
			Kind: ManifestErrorKindCorrupted,
			Err:  errors.New("wal_segment_max_bytes too small"),
		}
	}
	return nil
}

func mustBePositive(name string) error {
	return &ManifestError{
		Kind: ManifestErrorKindCorrupted,
		Err:  fmt.Errorf("%s must be positive", name),
	}
}

// LogDirOrDefault returns the configured log directory, or the default.
func (m *Manifest) LogDirOrDefault() string {
	if m.LogDir != nil {
		return *m.LogDir
	}
	return config.DefaultLogDir
}

// LogFileNameOrDefault returns the configured log file name, or the default.
func (m *Manifest) LogFileNameOrDefault() string {
	if m.LogFileName != nil {
		return *m.LogFileName
	}
	return config.DefaultLogFileName
}

// LogMaxSizeOrDefault returns the configured log max size (MB), or the default.
func (m *Manifest) LogMaxSizeOrDefault() int {
	if m.LogMaxSize != nil {
		return *m.LogMaxSize
	}
	return config.DefaultLogMaxSize
}

// LogMaxBackupsOrDefault returns the configured log max backups, or the default.
func (m *Manifest) LogMaxBackupsOrDefault() int {
	if m.LogMaxBackups != nil {
		return *m.LogMaxBackups
	}
	return config.DefaultLogMaxBackups
}

func (m *Manifest) ToMap() map[string]any {
	return map[string]any{
		"format_version":        m.FormatVersion,
		"fsync_on_commit":       m.FsyncOnCommit,
		"max_key_bytes":         m.MaxKeyBytes,
		"max_value_bytes":       m.MaxValueBytes,
		"wal_segment_max_bytes": m.WalSegmentMaxBytes,
		"wal_next_segment_id": func() any {
			if m.WalNextSegmentID != nil {
				return *m.WalNextSegmentID
			}
			return nil
		}(),
		"wal_log_max_bytes": func() any {
			if m.WalLogMaxBytes != nil {
				return *m.WalLogMaxBytes
			}
			return nil
		}(),
		"wal_log_max_backups": func() any {
			if m.WalLogMaxBackups != nil {
				return *m.WalLogMaxBackups
			}
			return nil
		}(),
		"log_dir":         m.LogDirOrDefault(),
		"log_file_name":   m.LogFileNameOrDefault(),
		"log_max_size":    m.LogMaxSizeOrDefault(),
		"log_max_backups": m.LogMaxBackupsOrDefault(),
	}
}
