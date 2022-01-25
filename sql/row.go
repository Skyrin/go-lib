package sql

import (
	"database/sql"
	"fmt"

	"github.com/Skyrin/go-lib/e"
)

const (
	ECode020201 = e.Code0202 + "01"
	ECode020202 = e.Code0202 + "02"
)

// Row a wrapper struct for sql.Row, so error handling can happen
type Row struct {
	row   *sql.Row
	query string
}

// Scan wrapper for row's Scan, which returns an extended error instead
func (r *Row) Scan(dest ...interface{}) error {
	if err := r.row.Scan(dest...); err != nil {
		return e.W(err, ECode020201, fmt.Sprintf("query: %s", r.query))
	}

	return nil
}

// Err wrapper for row's Err func
func (r *Row) Err() error {
	err := r.row.Err()
	if err == nil {
		return nil
	}

	return e.W(err, ECode020202, fmt.Sprintf("query: %s", r.query))
}
