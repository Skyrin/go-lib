package sql

import (
	"errors"

	gle "github.com/Skyrin/go-lib/errors"
	"github.com/lib/pq"
)

const (
	// PQErr23505UniqueViolation Postgres code for unique violation
	PQErr23505UniqueViolation = "23505"
	// PQErr58030IOError Postgres code for i/o error ("could not write to temporary file")
	PQErr58030IOError = "58030"
)

// IsPQError checks if the passed error is the specified Postgres error code
func IsPQError(err error, errorCode string) bool {
	var pqerr *pq.Error
	if ee := gle.AsExtendedError(err); ee != nil {
		return ee.AsError(&pqerr) && string(pqerr.Code) == errorCode
	}

	return errors.As(err, &pqerr) && string(pqerr.Code) == errorCode
}
