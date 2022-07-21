package sql

import (
	"database/sql"

	"github.com/Skyrin/go-lib/e"

	// Including postgres library for SQL connections
	_ "github.com/lib/pq"
)

const (
	ECode020801 = e.Code0208 + "01"
	ECode020802 = e.Code0208 + "02"
)

// Statement a prepared statement
type Statement struct {
	stmt *sql.Stmt
}

// Exec runs the prepared statement with the passed parameters
func (s *Statement) Exec(params ...interface{}) (res sql.Result, err error) {
	res, err = s.stmt.Exec(params...)
	if err != nil {
		return nil, e.W(err, ECode020801)
	}

	return res, nil
}

// Close closes the prepared statement
func (s *Statement) Close() (err error) {
	if err := s.stmt.Close(); err != nil {
		return e.W(err, ECode020802)
	}

	return nil
}
