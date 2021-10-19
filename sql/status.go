package sql

import (
	"fmt"

	"github.com/Skyrin/go-lib/e"
)

// Status defines a status reference for a table/column combination. The table/column/id
// should be unique (as well as the table/column/code).
type Status struct {
	ID     int
	Table  string
	Column string
	Code   string
	Name   string
}

// SetStatusLoader sets the status loader. This should load all statuses for the application
// presumably from the db, but could be defined in elsewhere, and return them as an array.
// The Connection will call this method when a status is first requested and
// cache the array into a map for access to the statuses per table/column combination
func (db *Connection) SetStatusLoad(f func(*Connection) ([]*Status, error)) {
	db.statusLoader = f
}

// statusRefLoad loads all status ref entries into the statusMap
func (db *Connection) statusRefLoad() (err error) {
	if db.statusLoader == nil {
		return e.New(e.Code020P, "01", "No status loader defined")
	}

	sList, err := db.statusLoader(db)
	if err != nil {
		return e.Wrap(err, e.Code020P, "02")
	}

	db.statusMap = make(map[string][]*Status, len(sList))
	for _, s := range sList {
		k := statusGetKey(s.Table, s.Column)
		db.statusMap[k] = append(db.statusMap[k], s)
	}

	return nil
}

// statusGetKey gets the key in the statusMap based on the table/column
func statusGetKey(table, column string) (key string) {
	return fmt.Sprintf("%s.%s", table, column)
}

// StatusGetByCode returns the status record associated with the table, column and code
// combination
func (db *Connection) StatusGetByCode(table, column, code string) (s *Status, err error) {
	if db.statusMap == nil {
		if err := db.statusRefLoad(); err != nil {
			return nil, e.Wrap(err, e.Code020Q, "01")
		}
	}

	tmpList, ok := db.statusMap[statusGetKey(table, column)]
	if !ok {
		return nil, e.New(e.Code020Q, "02",
			fmt.Sprintf("Invalid status table/column: %s/%s", table, column))
	}

	for _, s := range tmpList {
		if s.Code == code {
			return s, nil
		}
	}

	return nil, e.New(e.Code020Q, "03",
		fmt.Sprintf("Status does not exist: table: %s, col: %s, code: %s",
			table, column, code))
}

// StatusGetByID returns the status record associated with the table, column and id
// combination
func (db *Connection) StatusGetByID(table, column string, id int) (s *Status, err error) {
	if db.statusMap == nil {
		if err := db.statusRefLoad(); err != nil {
			return nil, e.Wrap(err, e.Code020R, "01")
		}
	}

	tmpList, ok := db.statusMap[statusGetKey(table, column)]
	if !ok {
		return nil, e.New(e.Code020R, "02",
			fmt.Sprintf("Invalid status table/column: %s/%s", table, column))
	}

	for _, s := range tmpList {
		if s.ID == id {
			return s, nil
		}
	}

	return nil, e.New(e.Code020R, "03",
		fmt.Sprintf("Status does not exist: table: %s, col: %s, id: %d",
			table, column, id))
}

// StatusGetListByTblAndCol returns all status records associated with the table/column
func (db *Connection) StatusGetListByTblAndCol(table, column string) (sList []*Status, err error) {
	if db.statusMap == nil {
		if err := db.statusRefLoad(); err != nil {
			return nil, e.Wrap(err, e.Code020S, "01")
		}
	}

	tmpList, ok := db.statusMap[statusGetKey(table, column)]
	if !ok {
		return nil, e.New(e.Code020S, "02",
			fmt.Sprintf("Invalid status table/column: %s/%s", table, column))
	}

	return tmpList, nil
}
