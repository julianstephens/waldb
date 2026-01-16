package db

import "errors"

// DB represents a WAL-based database instance.
type DB struct {
	path   string
	closed bool
}

// Open opens or creates a WAL database at the given path.
// This is a placeholder implementation.
func Open(path string) (*DB, error) {
	if path == "" {
		return nil, errors.New("path cannot be empty")
	}
	return &DB{
		path:   path,
		closed: false,
	}, nil
}

// Close closes the database.
// This is a placeholder implementation.
func (db *DB) Close() error {
	if db.closed {
		return errors.New("database already closed")
	}
	db.closed = true
	return nil
}

// Path returns the database path.
func (db *DB) Path() string {
	return db.path
}

// IsClosed returns true if the database is closed.
func (db *DB) IsClosed() bool {
	return db.closed
}
