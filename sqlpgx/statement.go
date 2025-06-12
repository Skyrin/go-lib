package sqlpgx

/*
import (
	"database/sql"
	"fmt"

	"github.com/Skyrin/go-lib/e"
	"github.com/jackc/pgx"
)

const (
	ECode020801 = e.Code0208 + "01"
	ECode020802 = e.Code0208 + "02"
)

// Statement a prepared statement
type Statement struct {
	stmt *pgx.PreparedStatement
}

// Exec runs the prepared statement with the passed parameters
func (s *Statement) Exec(params ...interface{}) (res sql.Result, err error) {
	commandTag, err := s.stmt.Exec(ctx, sql, id)
	if err != nil {
		return fmt.Errorf("error completing task: %w", err)
	}

	if commandTag.RowsAffected() == 0 {
		return fmt.Errorf("no task found with id %d", id)
	}

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
*/
