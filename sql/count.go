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

	ECode020101 = e.Code0201 + "01"
	ECode020102 = e.Code0201 + "02"
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
		return 0, e.W(err, ECode020101)
	}

	cntStmt := strings.Replace(stmt, FieldPlaceHolder, FieldCount, 1)
	row := c.QueryRow(cntStmt, bindParams...)
	if err := row.Scan(&count); err != nil {
		return 0, e.W(err, ECode020102,
			fmt.Sprintf("bindParams: %+v", bindParams))
	}

	return count, nil
}
