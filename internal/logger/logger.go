package logger

// Logger defines the interface for logging operations across waldb.
// All packages use this interface, allowing for flexible logger implementations.
type Logger interface {
	// Debug logs a debug-level message with optional structured fields.
	Debug(msg string, fields ...interface{})

	// Info logs an info-level message with optional structured fields.
	Info(msg string, fields ...interface{})

	// Warn logs a warning-level message with optional structured fields.
	Warn(msg string, fields ...interface{})

	// Error logs an error-level message with the error and optional structured fields.
	Error(msg string, err error, fields ...interface{})
}

// Closeable is an optional interface for loggers that need cleanup.
// Loggers that support graceful shutdown should implement this interface.
type Closeable interface {
	// Close gracefully closes the logger, flushing any pending messages.
	Close() error
}

// NoOpLogger is a no-operation logger that discards all messages.
// Used as the default logger for tests and when logging is disabled.
type NoOpLogger struct{}

// Debug is a no-op implementation.
func (NoOpLogger) Debug(string, ...interface{}) {}

// Info is a no-op implementation.
func (NoOpLogger) Info(string, ...interface{}) {}

// Warn is a no-op implementation.
func (NoOpLogger) Warn(string, ...interface{}) {}

// Error is a no-op implementation.
func (NoOpLogger) Error(string, error, ...interface{}) {}

// Verify NoOpLogger implements Logger interface
var _ Logger = NoOpLogger{}
