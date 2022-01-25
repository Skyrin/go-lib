package sql

import (
	"fmt"

	"github.com/Skyrin/go-lib/e"
)

const (
	ECode020401 = e.Code0204 + "01"
	ECode020402 = e.Code0204 + "02"
	ECode020403 = e.Code0204 + "03"
	ECode020404 = e.Code0204 + "04"
	ECode020405 = e.Code0204 + "05"
	ECode020406 = e.Code0204 + "06"
	ECode020407 = e.Code0204 + "07"
	ECode020408 = e.Code0204 + "08"
	ECode020409 = e.Code0204 + "09"
	ECode02040A = e.Code0204 + "0A"
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
		return e.N(ECode020401, "No status loader defined")
	}

	sList, err := db.statusLoader(db)
	if err != nil {
		return e.W(err, ECode020402)
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
			return nil, e.W(err, ECode020403)
		}
	}

	tmpList, ok := db.statusMap[statusGetKey(table, column)]
	if !ok {
		return nil, e.N(ECode020404,
			fmt.Sprintf("Invalid status table/column: %s/%s", table, column))
	}

	for _, s := range tmpList {
		if s.Code == code {
			return s, nil
		}
	}

	return nil, e.N(ECode020405,
		fmt.Sprintf("Status does not exist: table: %s, col: %s, code: %s",
			table, column, code))
}

// StatusGetByID returns the status record associated with the table, column and id
// combination
func (db *Connection) StatusGetByID(table, column string, id int) (s *Status, err error) {
	if db.statusMap == nil {
		if err := db.statusRefLoad(); err != nil {
			return nil, e.W(err, ECode020406)
		}
	}

	tmpList, ok := db.statusMap[statusGetKey(table, column)]
	if !ok {
		return nil, e.N(ECode020407,
			fmt.Sprintf("Invalid status table/column: %s/%s", table, column))
	}

	for _, s := range tmpList {
		if s.ID == id {
			return s, nil
		}
	}

	return nil, e.N(ECode020408,
		fmt.Sprintf("Status does not exist: table: %s, col: %s, id: %d",
			table, column, id))
}

// StatusGetListByTblAndCol returns all status records associated with the table/column
func (db *Connection) StatusGetListByTblAndCol(table, column string) (sList []*Status, err error) {
	if db.statusMap == nil {
		if err := db.statusRefLoad(); err != nil {
			return nil, e.W(err, ECode020409)
		}
	}

	tmpList, ok := db.statusMap[statusGetKey(table, column)]
	if !ok {
		return nil, e.N(ECode02040A,
			fmt.Sprintf("Invalid status table/column: %s/%s", table, column))
	}

	return tmpList, nil
}
