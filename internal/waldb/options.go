package waldb

// OpenOptions contains configuration for opening a WAL database.
//
// Temporary interface: Only FsyncOnCommit, LogDir, LogMaxSize, and LogMaxBak are
// persisted in the manifest (wal_log_* fields). Other logging options (console,
// log level, etc.) are transient CLI concerns and will be owned by the CLI layer
// as flags or environment variables until the full manifest schema is implemented.
type OpenOptions struct {
	// Durability configuration (persisted in manifest)
	FsyncOnCommit bool

	// File-based logging configuration (persisted in manifest as wal_log_*)
	LogDir     string // Directory for rotating log files (e.g., "./logs"); maps to wal_log_dir
	LogMaxSize int    // Max size per log file in MB (default: 100); maps to wal_log_max_size
	LogMaxBak  int    // Max number of backup log files (default: 5); maps to wal_log_max_backups
}
