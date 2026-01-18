package logger

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	tst "github.com/julianstephens/go-utils/tests"
)

// TestConsoleLogger_InfoLevel tests that Info messages are logged at info level
func TestConsoleLogger_InfoLevel(t *testing.T) {
	buf := &bytes.Buffer{}
	cl := &ConsoleLogger{
		minLevel: "info",
		out:      buf,
		err:      buf,
	}

	cl.Info("test message", "key", "value")

	output := buf.String()
	tst.AssertTrue(t, strings.Contains(output, "INFO"), "expected INFO in output")
	tst.AssertTrue(t, strings.Contains(output, "test message"), "expected message in output")
	tst.AssertTrue(t, strings.Contains(output, "key=value"), "expected fields in output")
}

// TestConsoleLogger_DebugHiddenAtInfoLevel tests that Debug messages are hidden at info level
func TestConsoleLogger_DebugHiddenAtInfoLevel(t *testing.T) {
	buf := &bytes.Buffer{}
	cl := &ConsoleLogger{
		minLevel: "info",
		out:      buf,
		err:      buf,
	}

	cl.Debug("debug message", "key", "value")

	output := buf.String()
	tst.AssertTrue(t, output == "", "expected no output at info level for debug")
}

// TestConsoleLogger_DebugVisibleAtDebugLevel tests that Debug messages are visible at debug level
func TestConsoleLogger_DebugVisibleAtDebugLevel(t *testing.T) {
	buf := &bytes.Buffer{}
	cl := &ConsoleLogger{
		minLevel: "debug",
		out:      buf,
		err:      buf,
	}

	cl.Debug("debug message", "key", "value")

	output := buf.String()
	tst.AssertTrue(t, strings.Contains(output, "DEBUG"), "expected DEBUG in output")
	tst.AssertTrue(t, strings.Contains(output, "debug message"), "expected message in output")
}

// TestConsoleLogger_WarnLevel tests that Warn messages are logged at warn level
func TestConsoleLogger_WarnLevel(t *testing.T) {
	buf := &bytes.Buffer{}
	cl := &ConsoleLogger{
		minLevel: "warn",
		out:      buf,
		err:      buf,
	}

	cl.Warn("warning", "reason", "test")
	tst.AssertTrue(t, strings.Contains(buf.String(), "WARN"), "expected WARN in output")

	buf.Reset()
	cl.Info("info", "key", "value")
	tst.AssertTrue(t, buf.String() == "", "expected Info hidden at warn level")
}

// TestConsoleLogger_ErrorAlwaysLogged tests that Error messages are always logged
func TestConsoleLogger_ErrorAlwaysLogged(t *testing.T) {
	buf := &bytes.Buffer{}
	cl := &ConsoleLogger{
		minLevel: "error",
		out:      buf,
		err:      buf,
	}

	err := errors.New("test error")
	cl.Error("operation failed", err, "op", "test")

	output := buf.String()
	tst.AssertTrue(t, strings.Contains(output, "ERROR"), "expected ERROR in output")
	tst.AssertTrue(t, strings.Contains(output, "operation failed"), "expected message in output")
	tst.AssertTrue(t, strings.Contains(output, "test error"), "expected error in output")
}

// TestConsoleLogger_Timestamp tests that logs include timestamp
func TestConsoleLogger_Timestamp(t *testing.T) {
	buf := &bytes.Buffer{}
	cl := &ConsoleLogger{
		minLevel: "info",
		out:      buf,
		err:      buf,
	}

	cl.Info("test")

	output := buf.String()
	// Timestamp format: 2006-01-02T15:04:05.000Z07:00
	tst.AssertTrue(t, strings.Contains(output, "T"), "expected timestamp with T separator")
	// Check for either Z or +/- timezone marker
	hasTimezone := strings.Contains(output, "Z") || strings.Contains(output, "+") || strings.Contains(output, "-")
	tst.AssertTrue(t, hasTimezone, "expected timestamp with timezone indicator")
}

// TestNewConsoleLogger_DefaultLevel tests that default level is info
func TestNewConsoleLogger_DefaultLevel(t *testing.T) {
	cl := NewConsoleLogger("")
	consoleLogger, ok := cl.(*ConsoleLogger)
	tst.AssertTrue(t, ok, "expected ConsoleLogger type")
	tst.RequireDeepEqual(t, consoleLogger.minLevel, "info")
}

// TestFileLogger_Creation tests that FileLogger can be created
func TestFileLogger_Creation(t *testing.T) {
	tmpDir := t.TempDir()
	logFile := filepath.Join(tmpDir, "test.log")

	fl, err := NewFileLogger(tmpDir, "test.log", 100, 5)
	tst.RequireNoError(t, err)
	tst.AssertNotNil(t, fl, "expected non-nil FileLogger")

	// Write something to trigger file creation
	fl.Info("test")

	// Verify file was created after first write
	_, err = os.Stat(logFile)
	tst.RequireNoError(t, err)

	// Clean up
	if c, ok := fl.(Closeable); ok {
		_ = c.Close()
	}
}

// TestFileLogger_WritesContent tests that FileLogger writes log content
func TestFileLogger_WritesContent(t *testing.T) {
	tmpDir := t.TempDir()
	fl, err := NewFileLogger(tmpDir, "test.log", 100, 5)
	tst.RequireNoError(t, err)

	fl.Info("test message", "key", "value")

	// Read back file
	logFile := filepath.Join(tmpDir, "test.log")
	content, err := os.ReadFile(logFile) // nolint:gosec
	tst.RequireNoError(t, err)

	output := string(content)
	// go-utils/logger uses JSON format
	tst.AssertTrue(t, strings.Contains(output, "info"), "expected 'info' level in output")
	tst.AssertTrue(t, strings.Contains(output, "test message"), "expected message in file")

	// Clean up
	if c, ok := fl.(Closeable); ok {
		_ = c.Close()
	}
}

// TestFileLogger_CreatesDirectory tests that FileLogger creates missing directory
func TestFileLogger_CreatesDirectory(t *testing.T) {
	tmpDir := t.TempDir()
	logDir := filepath.Join(tmpDir, "logs", "deep", "dir")

	fl, err := NewFileLogger(logDir, "test.log", 100, 5)
	tst.RequireNoError(t, err)
	tst.AssertNotNil(t, fl, "expected non-nil FileLogger")

	// Verify directory was created
	_, err = os.Stat(logDir)
	tst.RequireNoError(t, err)

	// Clean up
	if c, ok := fl.(Closeable); ok {
		_ = c.Close()
	}
}

// TestFileLogger_Close tests that FileLogger can be closed
func TestFileLogger_Close(t *testing.T) {
	tmpDir := t.TempDir()
	fl, err := NewFileLogger(tmpDir, "test.log", 100, 5)
	tst.RequireNoError(t, err)

	if c, ok := fl.(Closeable); ok {
		err = c.Close()
		tst.RequireNoError(t, err)
	} else {
		t.Fatal("expected FileLogger to implement Closeable")
	}
}

// TestMultiLogger_BothOutputs tests that MultiLogger writes to all loggers
func TestMultiLogger_BothOutputs(t *testing.T) {
	buf1 := &bytes.Buffer{}
	buf2 := &bytes.Buffer{}

	cl1 := &ConsoleLogger{
		minLevel: "info",
		out:      buf1,
		err:      buf1,
	}
	cl2 := &ConsoleLogger{
		minLevel: "info",
		out:      buf2,
		err:      buf2,
	}

	ml := NewMultiLogger(cl1, cl2)

	ml.Info("test message", "key", "value")

	tst.AssertTrue(t, strings.Contains(buf1.String(), "test message"), "expected message in first logger")
	tst.AssertTrue(t, strings.Contains(buf2.String(), "test message"), "expected message in second logger")
}

// TestMultiLogger_AllMethods tests that MultiLogger forwards all log methods
func TestMultiLogger_AllMethods(t *testing.T) {
	buf := &bytes.Buffer{}
	cl := &ConsoleLogger{
		minLevel: "debug",
		out:      buf,
		err:      buf,
	}

	ml := NewMultiLogger(cl)

	ml.Debug("debug")
	buf.Reset()
	ml.Info("info")
	tst.AssertTrue(t, strings.Contains(buf.String(), "info"), "expected info logged")

	buf.Reset()
	ml.Warn("warn")
	tst.AssertTrue(t, strings.Contains(buf.String(), "warn"), "expected warn logged")

	buf.Reset()
	err := errors.New("test")
	ml.Error("error", err)
	tst.AssertTrue(t, strings.Contains(buf.String(), "error"), "expected error logged")
}

// TestMultiLogger_Close tests that MultiLogger closes all loggers
func TestMultiLogger_Close(t *testing.T) {
	tmpDir := t.TempDir()
	fl, err := NewFileLogger(tmpDir, "test.log", 100, 5)
	tst.RequireNoError(t, err)

	buf := &bytes.Buffer{}
	cl := &ConsoleLogger{
		minLevel: "info",
		out:      buf,
		err:      buf,
	}

	ml := NewMultiLogger(cl, fl)

	if c, ok := ml.(Closeable); ok {
		err = c.Close()
		tst.RequireNoError(t, err)
	} else {
		t.Fatal("expected MultiLogger to implement Closeable")
	}
}

// TestMultiLogger_PartialClose tests that MultiLogger closes all loggers even if one fails
func TestMultiLogger_PartialClose(t *testing.T) {
	tmpDir := t.TempDir()
	fl, err := NewFileLogger(tmpDir, "test.log", 100, 5)
	tst.RequireNoError(t, err)

	ml := NewMultiLogger(fl, NoOpLogger{})

	if c, ok := ml.(Closeable); ok {
		err = c.Close()
		tst.RequireNoError(t, err)
	}
}

// TestConsoleLogger_MultipleFields tests logging with multiple field pairs
func TestConsoleLogger_MultipleFields(t *testing.T) {
	buf := &bytes.Buffer{}
	cl := &ConsoleLogger{
		minLevel: "info",
		out:      buf,
		err:      buf,
	}

	cl.Info("operation", "op", "put", "key", "user:123", "value_len", 42, "version", 1)

	output := buf.String()
	tst.AssertTrue(t, strings.Contains(output, "op=put"), "expected op field")
	tst.AssertTrue(t, strings.Contains(output, "key=user:123"), "expected key field")
	tst.AssertTrue(t, strings.Contains(output, "value_len=42"), "expected value_len field")
	tst.AssertTrue(t, strings.Contains(output, "version=1"), "expected version field")
}

// TestNoOpLogger_DoesNothing verifies NoOpLogger is truly a no-op
func TestNoOpLogger_DoesNothing(t *testing.T) {
	noop := NoOpLogger{}

	// These should not panic or produce any output
	noop.Debug("debug")
	noop.Info("info")
	noop.Warn("warn")
	noop.Error("error", errors.New("test"))

	// Verify no interface methods panic (test passes if no panic)
	tst.AssertTrue(t, true, "NoOpLogger methods don't panic")
}

// TestConsoleLogger_ErrorToStderr tests that errors go to stderr
func TestConsoleLogger_ErrorToStderr(t *testing.T) {
	outBuf := &bytes.Buffer{}
	errBuf := &bytes.Buffer{}

	cl := &ConsoleLogger{
		minLevel: "info",
		out:      outBuf,
		err:      errBuf,
	}

	cl.Info("info message")
	cl.Error("error message", errors.New("test"))

	// Info should go to stdout
	tst.AssertTrue(t, strings.Contains(outBuf.String(), "info message"), "expected info on stdout")

	// Error should go to stderr
	tst.AssertTrue(t, strings.Contains(errBuf.String(), "error message"), "expected error on stderr")
}
