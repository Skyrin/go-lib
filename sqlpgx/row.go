package sqlpgx

import (
	"fmt"

	"github.com/Skyrin/go-lib/e"
	"github.com/jackc/pgx/v5"
)

const (
	ECode090201 = e.Code0902 + "01"
	ECode090202 = e.Code0902 + "02"
)

// Row a wrapper struct for pgx.Row, so error handling can happen
type Row struct {
	row   *pgx.Row
	query string
}

// Scan wrapper for row's Scan, which returns an extended error instead
func (r *Row) Scan(dest ...interface{}) error {
	pr := *r.row
	if err := pr.Scan(dest...); err != nil {
		return e.W(err, ECode090201, fmt.Sprintf("query: %s", r.query))
	}

	return nil
}

// Err wrapper for row's Err func
func (r *Row) Err() error {
	// return e.W(err, ECode090202, fmt.Sprintf("query: %s", r.query))

	// pgx row does not have an error method
	return nil
}
