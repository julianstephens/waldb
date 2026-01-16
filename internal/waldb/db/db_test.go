package db_test

import (
	"testing"

	waldb "github.com/julianstephens/waldb/internal/waldb/db"
)

func TestOpenClose(t *testing.T) {
	db, err := waldb.Open("/tmp/test.db")
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	if db.Path() != "/tmp/test.db" {
		t.Errorf("expected path /tmp/test.db, got %s", db.Path())
	}

	if db.IsClosed() {
		t.Error("expected database to be open")
	}

	err = db.Close()
	if err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	if !db.IsClosed() {
		t.Error("expected database to be closed")
	}

	// Test double close
	err = db.Close()
	if err == nil {
		t.Error("expected error on double close")
	}
}

func TestOpenEmptyPath(t *testing.T) {
	_, err := waldb.Open("")
	if err == nil {
		t.Error("expected error for empty path")
	}
}
