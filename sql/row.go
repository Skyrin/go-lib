package sql

import (
	"database/sql"
	"fmt"

	"github.com/Skyrin/go-lib/e"
)

// Row a wrapper struct for sql.Row, so error handling can happen
type Row struct {
	row   *sql.Row
	query string
}

// Scan wrapper for row's Scan, which returns an extended error instead
func (r *Row) Scan(dest ...interface{}) error {
	if err := r.row.Scan(dest...); err != nil {
		return e.Wrap(err, e.Code020C, "01", fmt.Sprintf("query: %s", r.query))
	}

	return nil
}

// Err wrapper for row's Err func
func (r *Row) Err() error {
	err := r.row.Err()
	if err == nil {
		return nil
	}

	return e.Wrap(err, e.Code020D, "01", fmt.Sprintf("query: %s", r.query))
}
