package sqlpgx

import (
	"fmt"

	"github.com/Skyrin/go-lib/e"
	"github.com/jackc/pgx/v5"
)

const (
	ECode090601 = e.Code0906 + "01"
	ECode090602 = e.Code0906 + "02"
	ECode090603 = e.Code0906 + "03"
)

// Rows wrapper struct for sql.Rows, so error handling can happen
type Rows struct {
	rows  *pgx.Rows
	query string
}

// Scan wrapper for row's Scan, which returns an extended error instead
func (r *Rows) Scan(dest ...interface{}) error {
	pr := *r.rows
	if err := pr.Scan(dest...); err != nil {
		return e.W(err, ECode090601, fmt.Sprintf("query: %s", r.query))
	}

	return nil
}

// Err wrapper for row's Err func
func (r *Rows) Err() error {
	pr := *r.rows
	err := pr.Err()
	if err == nil {
		return nil
	}

	return e.W(err, ECode090602, fmt.Sprintf("query: %s", r.query))
}

// Close wrapper for row's Close func
// Leaving return error for compatibility
func (r *Rows) Close() error {
	pr := *r.rows
	pr.Close()

	return nil
}

// Next wrapper for row's Next func
func (r *Rows) Next() bool {
	pr := *r.rows
	return pr.Next()
}
