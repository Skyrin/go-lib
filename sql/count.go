package sql

import (
	"fmt"
	"strings"

	sq "github.com/Masterminds/squirrel"
	"github.com/Skyrin/go-lib/e"
)

const (
	// FieldPlaceHolder TODO: move to more generic location
	FieldPlaceHolder = "<FIELD_PLACE_HOLDER>"
	// FieldCount TODO: move to more generic location
	FieldCount = "count(*) AS cnt"
)

// QueryCount gets the count from a select builder query.
// Would prefer being able to generate the same query with
// different fields, but that doesn't seem possible with
// the current library being used.
// TODO: research alternatives or maybe fork/enhance as needed
func (c *Connection) QueryCount(sb sq.SelectBuilder) (count int, err error) {
	// Get the count before pplying an offset
	stmt, bindParams, err := sb.ToSql()
	if err != nil {
		return 0, e.Wrap(err, e.Code020T, "01")
	}

	cntStmt := strings.Replace(stmt, FieldPlaceHolder, FieldCount, 1)
	row := c.QueryRow(cntStmt, bindParams...)
	if err := row.Scan(&count); err != nil {
		return 0, e.Wrap(err, e.Code020T, "02",
			fmt.Sprintf("bindParams: %+v", bindParams))
	}

	return count, nil
}
