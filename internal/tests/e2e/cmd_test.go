package e2e_test

import (
	"errors"
	"path/filepath"
	"testing"

	tst "github.com/julianstephens/go-utils/tests"

	"github.com/julianstephens/waldb/internal/cli"
	"github.com/julianstephens/waldb/internal/logger"
	waldb_db "github.com/julianstephens/waldb/internal/waldb/db"
)

// assertErrLocked checks that err wraps ErrLocked.
func assertErrLocked(t *testing.T, err error, label string) {
	t.Helper()
	tst.AssertNotNil(t, err, label+": expected error when DB is locked")
	tst.AssertTrue(t, errors.Is(err, waldb_db.ErrLocked), label+": expected ErrLocked")
}

// ============================================================================
// CLI Command End-to-End Tests
// ============================================================================

// E2E: init → put → get → delete → get (key absent)
// Exercises the full happy-path command lifecycle via Run methods.
func TestCmd_InitPutGetDeleteGet(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "e2e-cmd")
	globals := cli.Globals{DB: dbPath}
	lg := logger.NoOpLogger{}

	// init
	initCmd := &cli.InitCmd{Force: false}
	err := initCmd.Run(globals, lg)
	tst.RequireNoError(t, err)

	// put
	putCmd := &cli.PutCmd{Key: "hello", Value: "world"}
	err = putCmd.Run(globals, lg)
	tst.RequireNoError(t, err)

	// get — key must be present
	getCmd := &cli.GetCmd{Key: "hello"}
	err = getCmd.Run(globals, lg)
	tst.RequireNoError(t, err)

	// delete
	delCmd := &cli.DelCmd{Key: "hello"}
	err = delCmd.Run(globals, lg)
	tst.RequireNoError(t, err)

	// get — key must be absent
	err = getCmd.Run(globals, lg)
	tst.AssertNotNil(t, err, "expected error after key deleted")
	tst.AssertTrue(t, errors.Is(err, waldb_db.ErrKeyNotFound), "expected ErrKeyNotFound")
}

// E2E: commands fail with ErrLocked when the DB is already open elsewhere,
// and succeed again once the lock is released.
func TestCmd_AdvisoryLock_CommandsFailWhenDBOpen(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "e2e-lock")
	globals := cli.Globals{DB: dbPath}
	lg := logger.NoOpLogger{}

	// Initialize the DB.
	err := (&cli.InitCmd{Force: false}).Run(globals, lg)
	tst.RequireNoError(t, err)

	// Hold the lock by keeping a DB instance open.
	holder, err := waldb_db.Open(dbPath, lg)
	tst.RequireNoError(t, err)

	// All commands that open the DB internally must fail with ErrLocked.
	assertErrLocked(t, (&cli.GetCmd{Key: "k"}).Run(globals, lg), "get")
	assertErrLocked(t, (&cli.PutCmd{Key: "k", Value: "v"}).Run(globals, lg), "put")
	assertErrLocked(t, (&cli.DelCmd{Key: "k"}).Run(globals, lg), "del")

	// Release the lock.
	tst.RequireNoError(t, holder.Close())

	// Commands must succeed once the lock is free.
	err = (&cli.PutCmd{Key: "k", Value: "v"}).Run(globals, lg)
	tst.RequireNoError(t, err)
}
