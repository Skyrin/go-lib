package sql

import (
	"database/sql"
	"fmt"

	"github.com/Skyrin/go-lib/e"
)

const (
	ECode020601 = e.Code0206 + "01"
	ECode020602 = e.Code0206 + "02"
	ECode020603 = e.Code0206 + "03"
)

// Rows wrapper struct for sql.Rows, so error handling can happen
type Rows struct {
	rows  *sql.Rows
	query string
}

// Scan wrapper for row's Scan, which returns an extended error instead
func (r *Rows) Scan(dest ...interface{}) error {
	if err := r.rows.Scan(dest...); err != nil {
		return e.W(err, ECode020601, fmt.Sprintf("query: %s", r.query))
	}

	return nil
}

// Err wrapper for row's Err func
func (r *Rows) Err() error {
	err := r.rows.Err()
	if err == nil {
		return nil
	}

	return e.W(err, ECode020602, fmt.Sprintf("query: %s", r.query))
}

// Close wrapper for row's Close func - returns extended error instead
func (r *Rows) Close() error {
	if err := r.rows.Close(); err != nil {
		return e.W(err, ECode020603, fmt.Sprintf("query: %s", r.query))
	}

	return nil
}

// Next wrapper for row's Next func
func (r *Rows) Next() bool {
	return r.rows.Next()
}
