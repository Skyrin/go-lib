package sqlpgx

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"

	"github.com/Skyrin/go-lib/e"
	"github.com/jackc/pgx/v5"
)

const (
	DefaultMaxParamPerUpdate  = 15000
	AbsoluteMaxParamPerUpdate = 65535

	ECode090901 = e.Code0909 + "01"
	ECode090902 = e.Code0909 + "02"
	ECode090903 = e.Code0909 + "03"
	ECode090904 = e.Code0909 + "04"
	ECode090905 = e.Code0909 + "05"
	ECode090906 = e.Code0909 + "06"
	ECode090907 = e.Code0909 + "07"
	ECode090908 = e.Code0909 + "08"
	ECode090909 = e.Code0909 + "09"
	// ECode09090A = e.Code0909 + "0A"
)

// BulkUpdate allows for multiple updates to be ran in a single query
type BulkUpdate struct {
	db                   *Connection
	maxParamPerStatement int             // The maximum number of parameters to send per statement
	table                string          // The name of the table to bulk update
	columns              []BulkUpdateCol // The column list to bulk update
	whereColumns         []string        // The list of columns to use in the where clause
	paramCount           int             // The current parameter count
	paramPerStatement    int             // The number of parameters per statement
	mutex                sync.RWMutex    // Mutex for thread safe adding to bulk update
	count                int             // Keeps track of current number of calls to Add, since last Flush
	total                int             // Keeps track of total number of calls to Add
	bindNumber           int             // Used when building the statement, keeping track of the current bind variable
	batch                *pgx.Batch
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
func NewBulkUpdate(ctx context.Context, db *Connection, table string,
	columns []BulkUpdateCol, whereColumns []string,
	useCache bool) (bu *BulkUpdate, err error) {

	if table == "" {
		return nil, e.N(ECode090901, "a table must be specified")
	}

	if len(columns) < 1 {
		return nil, e.N(ECode090902, "at least one column must be specified")
	}

	if len(whereColumns) < 1 {
		return nil, e.N(ECode090903, "at least one where column must be specified")
	}

	bu = &BulkUpdate{
		db:                   db,
		table:                table,
		columns:              columns,
		whereColumns:         whereColumns,
		maxParamPerStatement: DefaultMaxParamPerUpdate,
		paramPerStatement:    len(columns),
		mutex:                sync.RWMutex{},
		batch:                &pgx.Batch{},
	}

	// Initialize the builder
	bu.begin(ctx)

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

// GetCount returns the number of rows that have been added to the bulk update since
// initialization or the last Flush call
func (bu *BulkUpdate) GetCount() (count int) {
	return bu.count
}

// GetAbsoluteMaxParamPerUpdate returns the maximum number of columns that is allowed to be added to the bulk update
func (bu *BulkUpdate) GetAbsoluteMaxParamPerUpdate() (count int) {
	return AbsoluteMaxParamPerUpdate
}

// GetParamPerStatement returns the number of params that have been assigned to each statement
func (bu *BulkUpdate) GetParamPerStatement() (count int) {
	return bu.paramPerStatement
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
func (bu *BulkUpdate) Add(ctx context.Context, values ...interface{}) (rowsUpdated int, err error) {
	bu.mutex.Lock()
	defer func() {
		bu.mutex.Unlock()
	}()

	bu.count++
	bu.total++

	if len(values) != bu.paramPerStatement {
		return 0, e.N(ECode090904, "number of values must equal number of columns")
	}

	bu.batch.Queue(bu.build(), values...)

	// Increment the param count
	bu.paramCount += bu.paramPerStatement

	// If the param count exceeds the max param per update, then run the query now
	if bu.paramCount > bu.maxParamPerStatement {
		// Run the currently stored statement
		if err := bu.exec(ctx); err != nil {
			return 0, e.W(err, ECode090905)
		}

		// Get the number of rows that were updated (should be the current count)
		// Ensure this is done before the begin call, as that will reset the count
		rowsUpdated = bu.count

		// Reset the param count and update builder
		bu.begin(ctx)
	}

	return rowsUpdated, nil
}

// Close the batch
func (bu *BulkUpdate) Close() (errList []error) {
	if bu.batch == nil {
		return nil
	}

	bu.mutex.Lock()
	defer func() {
		bu.mutex.Unlock()
	}()

	bu.batch = &pgx.Batch{}

	return errList
}

// Flush if there is a remaining statement to run, it will
// execute the query
func (bu *BulkUpdate) Flush(ctx context.Context) (err error) {
	bu.mutex.Lock()
	defer func() {
		bu.mutex.Unlock()
	}()

	if bu.paramCount > 0 {
		if err := bu.exec(ctx); err != nil {
			return e.W(err, ECode090906)
		}
	}

	bu.begin(ctx)

	return nil
}

// begin resets the param list, param count and count
func (bu *BulkUpdate) begin(ctx context.Context) {
	bu.paramCount = 0
	bu.count = 0

	bu.Close()
}

// exec runs the update statement
func (bu *BulkUpdate) exec(ctx context.Context) (err error) {
	// Begin a transaction
	tx, err := bu.db.DB.Begin(ctx)
	if err != nil {
		return e.W(err, ECode090907, "error starting transaction")
	}
	defer tx.Rollback(ctx)

	count := bu.batch.Len()

	// Send the batch
	results := tx.SendBatch(ctx, bu.batch)
	defer results.Close()

	// Check for errors in the results
	for i := 0; i < count; i++ {
		_, err := results.Exec()
		if err != nil {
			msg := fmt.Sprintf("error executing batch command %d: %w", i, err)

			return e.N(ECode090908, msg)
		}
	}

	// Commit the transaction
	if err = tx.Commit(ctx); err != nil {
		return e.W(err, ECode090909, "error committing transaction")
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
