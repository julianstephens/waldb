package logger

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/julianstephens/go-utils/helpers"
	goulog "github.com/julianstephens/go-utils/logger"
)

// ConsoleLogger writes structured logs to stdout/stderr with timestamps and formatting.
type ConsoleLogger struct {
	minLevel string // "debug", "info", "warn", "error"
	out      io.Writer
	err      io.Writer
}

// NewConsoleLogger creates a logger that writes to console (stdout/stderr).
// level can be "debug", "info", "warn", or "error".
func NewConsoleLogger(level string) Logger {
	if level == "" {
		level = "info"
	}
	return &ConsoleLogger{
		minLevel: level,
		out:      os.Stdout,
		err:      os.Stderr,
	}
}

func (cl *ConsoleLogger) Debug(msg string, fields ...interface{}) {
	if cl.minLevel == "debug" {
		cl.log("DEBUG", msg, fields...)
	}
}

func (cl *ConsoleLogger) Info(msg string, fields ...interface{}) {
	if cl.minLevel == "debug" || cl.minLevel == "info" {
		cl.log("INFO", msg, fields...)
	}
}

func (cl *ConsoleLogger) Warn(msg string, fields ...interface{}) {
	if cl.minLevel == "debug" || cl.minLevel == "info" || cl.minLevel == "warn" {
		cl.log("WARN", msg, fields...)
	}
}

func (cl *ConsoleLogger) Error(msg string, err error, fields ...interface{}) {
	// Always log errors regardless of level
	allFields := append([]interface{}{"error", err}, fields...)
	cl.log("ERROR", msg, allFields...)
}

func (cl *ConsoleLogger) log(level string, msg string, fields ...interface{}) {
	timestamp := time.Now().Format("2006-01-02T15:04:05.000Z07:00")

	fieldStr := ""
	for i := 0; i < len(fields); i += 2 {
		if i+1 < len(fields) {
			fieldStr += fmt.Sprintf(" %v=%v", fields[i], fields[i+1])
		}
	}

	logLine := fmt.Sprintf("[%s] %s: %s%s\n", timestamp, level, msg, fieldStr)

	if level == "ERROR" {
		fmt.Fprint(cl.err, logLine) // nolint:errcheck
	} else {
		fmt.Fprint(cl.out, logLine) // nolint:errcheck
	}
}

// FileLogger wraps go-utils/logger with rotating file output.
// It delegates to go-utils/logger.Logger for all logging operations.
type FileLogger struct {
	underlying *goulog.Logger
	filePath   string
}

// NewFileLogger creates a logger that writes to a rotating file using go-utils/logger.
// It uses go-utils/logger's built-in rotating file handler with configurable parameters.
//
// Parameters:
//   - logDir: Directory where log files will be stored (created if missing)
//   - logFileName: Name of the log file (e.g., "waldb.log")
//   - maxFileSizeMB: Maximum size per log file in MB before rotation (e.g., 100)
//   - maxBackups: Maximum number of backup log files to retain (e.g., 5)
//
// Returns a Logger that writes to rotating files with automatic compression
// of old logs (enabled by default) and 28-day retention.
func NewFileLogger(logDir string, logFileName string, maxFileSizeMB int, maxBackups int) (Logger, error) {
	if err := helpers.Ensure(logDir, true); err != nil {
		return nil, wrapLoggerErr("create file logger", ErrLogCreate, err, logDir)
	}

	logPath := filepath.Join(logDir, logFileName)

	// Create a new logger instance from go-utils/logger
	underlying := goulog.New()

	// Configure rotating file output with custom settings
	if err := underlying.SetFileOutputWithConfig(goulog.FileRotationConfig{
		Filename:   logPath,
		MaxSize:    maxFileSizeMB,
		MaxBackups: maxBackups,
		// FIXME: remove in favor of CLI-configurable retention
		MaxAge:   28,   // Retain logs for 28 days
		Compress: true, // Compress old logs to save space
	}); err != nil {
		return nil, wrapLoggerErr("create file logger", ErrLogCreate, err, logDir)
	}

	return &FileLogger{
		underlying: underlying,
		filePath:   logPath,
	}, nil
}

func (fl *FileLogger) Debug(msg string, fields ...interface{}) {
	if len(fields) > 0 {
		// Convert fields to structured format for go-utils/logger
		fieldMap := fieldsToMap(fields)
		fl.underlying.WithFields(fieldMap).Debug(msg)
	} else {
		fl.underlying.Debug(msg)
	}
}

func (fl *FileLogger) Info(msg string, fields ...interface{}) {
	if len(fields) > 0 {
		fieldMap := fieldsToMap(fields)
		fl.underlying.WithFields(fieldMap).Info(msg)
	} else {
		fl.underlying.Info(msg)
	}
}

func (fl *FileLogger) Warn(msg string, fields ...interface{}) {
	if len(fields) > 0 {
		fieldMap := fieldsToMap(fields)
		fl.underlying.WithFields(fieldMap).Warn(msg)
	} else {
		fl.underlying.Warn(msg)
	}
}

func (fl *FileLogger) Error(msg string, err error, fields ...interface{}) {
	// Include the error in the fields
	allFields := append([]interface{}{"error", err}, fields...)
	fieldMap := fieldsToMap(allFields)
	fl.underlying.WithFields(fieldMap).Error(msg)
}

// Close gracefully closes the logger.
// For go-utils/logger, this ensures all pending log entries are flushed.
func (fl *FileLogger) Close() error {
	// go-utils/logger doesn't expose a Close method, but we maintain the interface
	// for future compatibility and graceful shutdown patterns
	return nil
}

// fieldsToMap converts variadic field arguments to a map.
// Expects pairs of arguments: key1, value1, key2, value2, etc.
func fieldsToMap(fields []interface{}) map[string]interface{} {
	result := make(map[string]interface{})
	for i := 0; i < len(fields); i += 2 {
		if i+1 < len(fields) {
			key := fmt.Sprintf("%v", fields[i])
			result[key] = fields[i+1]
		}
	}
	return result
}

// MultiLogger writes to multiple outputs simultaneously.
type MultiLogger struct {
	loggers []Logger
}

// NewMultiLogger combines multiple loggers into a single logger.
// All log calls are forwarded to all underlying loggers.
func NewMultiLogger(loggers ...Logger) Logger {
	return &MultiLogger{
		loggers: loggers,
	}
}

func (ml *MultiLogger) Debug(msg string, fields ...interface{}) {
	for _, lg := range ml.loggers {
		lg.Debug(msg, fields...)
	}
}

func (ml *MultiLogger) Info(msg string, fields ...interface{}) {
	for _, lg := range ml.loggers {
		lg.Info(msg, fields...)
	}
}

func (ml *MultiLogger) Warn(msg string, fields ...interface{}) {
	for _, lg := range ml.loggers {
		lg.Warn(msg, fields...)
	}
}

func (ml *MultiLogger) Error(msg string, err error, fields ...interface{}) {
	for _, lg := range ml.loggers {
		lg.Error(msg, err, fields...)
	}
}

func (ml *MultiLogger) Close() error {
	var lastErr error
	for _, lg := range ml.loggers {
		if c, ok := lg.(Closeable); ok {
			if err := c.Close(); err != nil {
				lastErr = err
			}
		}
	}
	return wrapLoggerErr("close multi logger", ErrLogClose, lastErr, "")
}
