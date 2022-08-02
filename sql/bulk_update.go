package sql

import (
	dsql "database/sql"
	"strconv"
	"strings"
	"sync"

	"github.com/Masterminds/squirrel"
	"github.com/Skyrin/go-lib/e"
)

const (
	DefaultMaxParamPerUpdate  = 15000
	AbsoluteMaxParamPerUpdate = 65535

	ECode020901 = e.Code0209 + "01"
	ECode020902 = e.Code0209 + "02"
	ECode020903 = e.Code0209 + "03"
	ECode020904 = e.Code0209 + "04"
	ECode020905 = e.Code0209 + "05"
	ECode020906 = e.Code0209 + "06"
	ECode020907 = e.Code0209 + "07"
	ECode020908 = e.Code0209 + "08"
	ECode020909 = e.Code0209 + "09"
	ECode02090A = e.Code0209 + "0A"
)

// BulkUpdate allows for multiple updates to be ran in a single query
type BulkUpdate struct {
	db                   *Connection
	maxParamPerStatement int                    // The maximum number of parameters to send per statement
	table                string                 // The name of the table to bulk update
	columns              []BulkUpdateCol        // The column list to bulk update
	whereColumns         []string               // The list of columns to use in the where clause
	ib                   squirrel.UpdateBuilder // The current update builder
	bindParamList        []interface{}          // The current list of parameters to bind to the statement
	paramCount           int                    // The current parameter count
	paramPerStatement    int                    // The number of parameters per statement
	cache                map[int]*dsql.Stmt     // Stores cached statements, if enabled
	enableCache          bool                   // Indicate whether to enable cache or not
	mutex                sync.RWMutex           // Mutex for thread safe adding to bulk update
	count                int                    // Keeps track of current number of calls to Add, since last Flush
	total                int                    // Keeps track of total number of calls to Add
	bindNumber           int                    // Used when building the statement, keeping track of the current bind variable
}

// BulkUpdateCol defines the column name and type. If type is left empty, it will not be specified in the
// update query. If it is specified, it must be a valid Postgres type in the database and inserted values
// will automatically be cast to that type
type BulkUpdateCol struct {
	Name string
	Type string
}

// NewBulkUpdate initializes a new BulkUpdate, specifying the table, columns to update, columns to use as filters
// and whether to use caching or not
func NewBulkUpdate(db *Connection, table string,
	columns []BulkUpdateCol, whereColumns []string,
	useCache bool) (bu *BulkUpdate, err error) {

	if table == "" {
		return nil, e.N(ECode020901, "a table must be specified")
	}

	if len(columns) < 1 {
		return nil, e.N(ECode020902, "at least one column must be specified")
	}

	if len(whereColumns) < 1 {
		return nil, e.N(ECode020903, "at least one where column must be specified")
	}

	bu = &BulkUpdate{
		db:                   db,
		table:                table,
		columns:              columns,
		whereColumns:         whereColumns,
		maxParamPerStatement: DefaultMaxParamPerUpdate,
		paramPerStatement:    len(columns),
		enableCache:          useCache,
		mutex:                sync.RWMutex{},
		bindParamList:        make([]interface{}, 0),
	}

	if useCache {
		bu.EnableCache()
	}

	// Initialize the builder
	bu.begin()

	return bu, nil
}

// SetMaxParamPerUpdate sets the max params to use per update. If this value is greater than the absolute
// maximum, then it will silently set it to the absolute maximum instead
func (bu *BulkUpdate) SetMaxParamPerUpdate(i int) {
	if i > AbsoluteMaxParamPerUpdate {
		i = AbsoluteMaxParamPerUpdate
	}

	bu.maxParamPerStatement = i
}

// EnableCache enables caching of bulk update statements. If used, Close must be called when finished
// to properly clean up the sql connections
func (bu *BulkUpdate) EnableCache() {
	bu.enableCache = true
	bu.cache = make(map[int]*dsql.Stmt)
}

// DisableCache disables the cache and closes any open statements
func (bu *BulkUpdate) DisableCache() (errList []error) {
	bu.enableCache = false
	bu.cache = nil
	return bu.Close()
}

// GetCount returns the number of rows that have been added to the bulk update since
// initialization or the last Flush call
func (bu *BulkUpdate) GetCount() (count int) {
	return bu.count
}

// GetTotal returns the total number of rows that have been added to the bulk update
// since it was initialized
func (bu *BulkUpdate) GetTotal() (total int) {
	bu.mutex.RLock()
	defer func() {
		bu.mutex.RUnlock()
	}()
	return bu.total
}

// Add adds the values to be sent as a bulk update. If the number of parameters
// exceeds the max param per update, then it will run the currently build statement
// and then reset itself for more values to be added. It will return the number of
// rows that were updated, or zero if the query was not executed
func (bu *BulkUpdate) Add(values ...interface{}) (rowsUpdated int, err error) {
	bu.mutex.Lock()
	defer func() {
		bu.mutex.Unlock()
	}()

	bu.count++
	bu.total++

	if len(values) != bu.paramPerStatement {
		return 0, e.N(ECode020904, "number of values must equal number of columns")
	}

	// Append the values to the bind list
	bu.bindParamList = append(bu.bindParamList, values...)
	// bu.ib = bu.ib.Values(values...)

	// Increment the param count
	bu.paramCount += bu.paramPerStatement

	// If the param count exceeds the max param per update, then run the query now
	if bu.paramCount > bu.maxParamPerStatement {
		// Run the currently stored statement
		if err := bu.exec(); err != nil {
			return 0, e.W(err, ECode020905)
		}

		// Get the number of rows that were updated (should be the current count)
		// Ensure this is done before the begin call, as that will reset the count
		rowsUpdated = bu.count

		// Reset the param count and update builder
		bu.begin()
	}

	return rowsUpdated, nil
}

// Close if cache is enabled, then it closes all cached statements
func (bu *BulkUpdate) Close() (errList []error) {
	if bu.cache == nil {
		return nil
	}
	bu.mutex.Lock()
	defer func() {
		bu.mutex.Unlock()
	}()

	for key, stmt := range bu.cache {
		if err := stmt.Close(); err != nil {
			errList = append(errList, err)
		}
		delete(bu.cache, key)
	}

	return errList
}

// Flush if there is a remaining statement to run, it will
// execute the query
func (bu *BulkUpdate) Flush() (err error) {
	bu.mutex.Lock()
	defer func() {
		bu.mutex.Unlock()
	}()

	if bu.paramCount > 0 {
		if err := bu.exec(); err != nil {
			return e.W(err, ECode020906)
		}
	}

	bu.begin()
	return nil
}

// begin resets the param list, param count and count
func (bu *BulkUpdate) begin() {
	// Possible optimization: reuse a bind param list once made
	bu.bindParamList = make([]interface{}, 0)
	bu.paramCount = 0
	bu.count = 0
}

// exec runs the update statement
func (bu *BulkUpdate) exec() (err error) {
	if bu.enableCache {
		// Statements only change based on the nubmer of parameters. So, the cache is
		// keyed off of the current parameter count
		_, ok := bu.cache[bu.paramCount]
		if !ok {
			stmt, err := bu.db.Prepare(bu.build())
			if err != nil {
				return e.W(err, ECode020907)
			}
			bu.cache[bu.paramCount] = stmt
		}

		_, err = bu.cache[bu.paramCount].Exec(bu.bindParamList...)
		if err != nil {
			return e.W(err, ECode020908)
		}
	} else {
		stmt, err := bu.db.Prepare(bu.build())
		if err != nil {
			return e.W(err, ECode020909)
		}
		if _, err = stmt.Exec(bu.bindParamList...); err != nil {
			return e.W(err, ECode02090A)
		}
	}

	return nil
}

// build creates the statement based on the columns, where columns and current number of bind values
func (bu *BulkUpdate) build() (stmt string) {
	sb := &strings.Builder{}
	_, _ = sb.WriteString("UPDATE ")
	_, _ = sb.WriteString(bu.table)
	_, _ = sb.WriteString(" AS t1 SET ")

	// Write first column
	_, _ = sb.WriteString(bu.columns[0].Name)
	_, _ = sb.WriteString("=t2.")
	_, _ = sb.WriteString(bu.columns[0].Name)

	//Write the remaining columns
	for i := 1; i < len(bu.columns); i++ {
		_, _ = sb.WriteString(",")
		_, _ = sb.WriteString(bu.columns[i].Name)
		_, _ = sb.WriteString("=t2.")
		_, _ = sb.WriteString(bu.columns[i].Name)
	}

	_, _ = sb.WriteString(" FROM (VALUES")

	// Build the first set
	bu.bindNumber = 1
	bu.buildValue(sb)

	// Build the rest
	for i := 1; i < bu.count; i++ {
		_, _ = sb.WriteString(",")
		bu.buildValue(sb)
	}

	_, _ = sb.WriteString(") AS t2(")
	_, _ = sb.WriteString(bu.columns[0].Name)
	for i := 1; i < len(bu.columns); i++ {
		_, _ = sb.WriteString(",")
		_, _ = sb.WriteString(bu.columns[i].Name)
	}
	_, _ = sb.WriteString(") WHERE ")

	// Write first where clause
	_, _ = sb.WriteString("t1.")
	_, _ = sb.WriteString(bu.whereColumns[0])
	_, _ = sb.WriteString("=t2.")
	_, _ = sb.WriteString(bu.whereColumns[0])

	for i := 1; i < len(bu.whereColumns); i++ {
		_, _ = sb.WriteString(" AND ")
		_, _ = sb.WriteString("t1.")
		_, _ = sb.WriteString(bu.whereColumns[i])
		_, _ = sb.WriteString("=t2.")
		_, _ = sb.WriteString(bu.whereColumns[i])
	}

	return sb.String()
}

// buildValue creates one set of bind variables to be updated
func (bu *BulkUpdate) buildValue(sb *strings.Builder) {
	_, _ = sb.WriteString(" ($")
	_, _ = sb.WriteString(strconv.Itoa(bu.bindNumber))
	if bu.columns[0].Type != "" {
		_, _ = sb.WriteString("::")
		_, _ = sb.WriteString(bu.columns[0].Type)
	}
	bu.bindNumber++
	for j := 1; j < bu.paramPerStatement; j++ {
		_, _ = sb.WriteString(",$")
		_, _ = sb.WriteString(strconv.Itoa(bu.bindNumber))
		if bu.columns[j].Type != "" {
			_, _ = sb.WriteString("::")
			_, _ = sb.WriteString(bu.columns[j].Type)
		}
		bu.bindNumber++
	}
	_, _ = sb.WriteString(")")
}
