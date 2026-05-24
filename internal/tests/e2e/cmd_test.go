package e2e_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	tst "github.com/julianstephens/go-utils/tests"

	"github.com/julianstephens/waldb/internal/logger"
	waldb_db "github.com/julianstephens/waldb/internal/waldb/db"
)

// ============================================================================
// CLI Command End-to-End Tests (real binary / real flag parsing)
// ============================================================================

type cmdResult struct {
	Stdout   string
	Stderr   string
	ExitCode int
}

// runWaldb executes the waldb binary with the given args and returns the
// combined output and exit code. WALDB_CONSOLE_ONLY=true is always injected so
// the binary writes logs to stderr instead of creating log files on disk.
func runWaldb(t *testing.T, args ...string) cmdResult {
	t.Helper()
	cmd := exec.Command(waldbBin, args...) //nolint:gosec // G204: waldbBin is a test binary path built by TestMain
	cmd.Env = append(os.Environ(), "WALDB_CONSOLE_ONLY=true")
	var stdout, stderr strings.Builder
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	code := 0
	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			code = exitErr.ExitCode()
		} else {
			t.Fatalf("unexpected exec error: %v", err)
		}
	}
	return cmdResult{Stdout: stdout.String(), Stderr: stderr.String(), ExitCode: code}
}

// combined returns stdout and stderr merged for error-message assertions.
func (r cmdResult) combined() string { return r.Stdout + r.Stderr }

// E2E: init → put → get → delete → get via real CLI parsing.
// Verifies flag parsing, command dispatch, and exit codes end-to-end.
func TestCmd_InitPutGetDeleteGet(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "e2e-cmd")

	// init
	r := runWaldb(t, "--db", dbPath, "init")
	tst.AssertEqual(t, 0, r.ExitCode, "init: expected exit 0")

	// put
	r = runWaldb(t, "--db", dbPath, "put", "hello", "world")
	tst.AssertEqual(t, 0, r.ExitCode, "put: expected exit 0")

	// get — key must be present
	r = runWaldb(t, "--db", dbPath, "get", "hello")
	tst.AssertEqual(t, 0, r.ExitCode, "get: expected exit 0")
	tst.AssertTrue(t, strings.Contains(r.combined(), "world"), "get: expected value in output")

	// delete
	r = runWaldb(t, "--db", dbPath, "del", "hello")
	tst.AssertEqual(t, 0, r.ExitCode, "del: expected exit 0")

	// get — key must be absent → non-zero exit
	r = runWaldb(t, "--db", dbPath, "get", "hello")
	tst.AssertTrue(t, r.ExitCode != 0, "get after delete: expected non-zero exit")
	tst.AssertTrue(
		t,
		strings.Contains(r.combined(), "key not found"),
		"get after delete: expected key not found message",
	)
}

// E2E: commands fail when the DB advisory lock is held by another process,
// and succeed again once the lock is released.
func TestCmd_AdvisoryLock_CommandsFailWhenDBOpen(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "e2e-lock")

	r := runWaldb(t, "--db", dbPath, "init")
	tst.AssertEqual(t, 0, r.ExitCode, "init: expected exit 0")

	// Hold the advisory lock in-process; the subprocess cannot acquire it.
	holder, err := waldb_db.Open(dbPath, logger.NoOpLogger{})
	tst.RequireNoError(t, err)

	for _, tc := range []struct {
		name string
		args []string
	}{
		{"get", []string{"--db", dbPath, "get", "k"}},
		{"put", []string{"--db", dbPath, "put", "k", "v"}},
		{"del", []string{"--db", dbPath, "del", "k"}},
	} {
		r = runWaldb(t, tc.args...)
		tst.AssertTrue(t, r.ExitCode != 0, tc.name+": expected non-zero exit when DB is locked")
		tst.AssertTrue(t, strings.Contains(r.combined(), "locked"), tc.name+": expected locked error in output")
	}

	// Release the lock — subsequent commands must succeed.
	tst.RequireNoError(t, holder.Close())

	r = runWaldb(t, "--db", dbPath, "put", "k", "v")
	tst.AssertEqual(t, 0, r.ExitCode, "put after lock release: expected exit 0")
}
