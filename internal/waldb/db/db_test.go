package db_test

import (
	"testing"

	tst "github.com/julianstephens/go-utils/tests"
	waldb "github.com/julianstephens/waldb/internal/waldb/db"
)

func TestOpenClose(t *testing.T) {
	db, err := waldb.Open("/tmp/test.db")
	tst.RequireNoError(t, err)

	tst.RequireDeepEqual(t, db.Path(), "/tmp/test.db")

	tst.AssertFalse(t, db.IsClosed(), "expected database to be open")

	err = db.Close()
	tst.RequireNoError(t, err)

	tst.AssertTrue(t, db.IsClosed(), "expected database to be closed")

	// Test double close
	err = db.Close()
	tst.AssertNotNil(t, err, "expected error on double close")
}

func TestOpenEmptyPath(t *testing.T) {
	_, err := waldb.Open("")
	tst.AssertNotNil(t, err, "expected error for empty path")
}
