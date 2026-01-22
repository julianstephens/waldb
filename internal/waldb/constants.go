package waldb

const (
	DefaultSegmentMaxBytes int64 = 256 * 1024 * 1024
)

// Log file defaults
const (
	DefaultAppDir        = ".waldb"
	DefaultLogDir        = "logs"
	DefaultLogFileName   = "waldb.log"
	DefaultLogMaxSize    = 100
	DefaultLogMaxBackups = 3
	DefaultLogLevel      = "info"
)

const (
	Version = "0.1.0"
)
