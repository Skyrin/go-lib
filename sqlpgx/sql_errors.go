package sqlpgx

import (
	"errors"
	"fmt"

	"github.com/Skyrin/go-lib/e"
	"github.com/jackc/pgx/v5/pgconn"
)

const (
	PQErr23505UniqueViolation = "23505" // Postgres code for unique_violation
	PQErr58030IOError         = "58030" // Postgres code for i/o error ("could not write to temporary file")
	PQErr23503IOError         = "23503" // Postgres code for foreign_key_violation
)

// IsPQError checks if the passed error is the specified Postgres error code
func IsPQError(err error, errorCode string) bool {
	var pgErr *pgconn.PgError
	if ee := e.AsExtendedError(err); ee != nil {
		return ee.AsError(&pgErr) && string(pgErr.Code) == errorCode
	}

	return errors.As(err, &pgErr) && string(pgErr.Code) == errorCode
}

// CheckPQError checks if it's a known postgres error and returns a proper message
func CheckPQError(err error) error {
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) {
			// fmt.Printf("PostgreSQL error: %s (Code: %s)\n", pgErr.Message, pgErr.Code)
			// Handle specific error codes
			switch pgErr.Code {
			case "23505": // unique_violation
				return fmt.Errorf("record already exists: %w", err)
			case "23503": // foreign_key_violation
				return fmt.Errorf("referenced record does not exist: %w", err)
			default:
				return fmt.Errorf("database error: %w", err)
			}
		}
		return fmt.Errorf("unexpected error: %w", err)
	}

	return nil
}
