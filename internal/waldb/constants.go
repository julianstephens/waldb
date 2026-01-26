package waldb

const (
	DefaultSegmentMaxBytes int64 = 256 * 1024 * 1024
)

// Log file defaults
const (
	DefaultLogDir        = "logs"
	DefaultLogFileName   = "waldb.log"
	DefaultLogMaxSize    = 100
	DefaultLogMaxBackups = 3
	DefaultLogLevel      = "info"
)

// Database versioning
const (
	Version         = "0.1.0"
	ManifestVersion = 1
)

// File and directory names
const (
	LockFileName     = "LOCK"
	ManifestFileName = "MANIFEST.json"
	WALDirName       = "wal"
)
