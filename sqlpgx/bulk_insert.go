package sqlpgx

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/Skyrin/go-lib/e"
	"github.com/jackc/pgx/v5"
)

const (
	DefaultMaxParamPerInsert  = 15000
	AbsoluteMaxParamPerInsert = 65535

	ECode090701 = e.Code0907 + "01"
	ECode090702 = e.Code0907 + "02"
	ECode090703 = e.Code0907 + "03"
	ECode090704 = e.Code0907 + "04"
	ECode090705 = e.Code0907 + "05"
	ECode090706 = e.Code0907 + "06"
	ECode090707 = e.Code0907 + "07"
	ECode090708 = e.Code0907 + "08"
	ECode090709 = e.Code0907 + "09"
	ECode09070A = e.Code0907 + "0A"
	ECode09070B = e.Code0907 + "0B"
)

// BulkInsert allows for multiple inserts to be ran in a single query, speeding up
// inserts into a table.
type BulkInsert struct {
	db                *Connection
	maxParamPerInsert int             // The maximum number of parameters to send per insert
	Table             string          // The name of the table to bulk insert into
	Columns           string          // The column list to bulk insert
	Suffix            string          // Optional suffix to append to a bulk insert (e.g. ON CONFLICT DO NOTHING)
	paramCount        int             // The current parameter count
	paramPerStatement int             // The number of parameters per statement
	preInsert         func() error    // Called immediately before an insert is executed
	postInsert        func(int) error // Called after an insert has been executed
	batch             *pgx.Batch
	mutex             sync.RWMutex // Mutex for thread safe adding to bulk insert
	count             int          // Keeps track of current number of calls to Add, since last Flush
	total             int          // Keeps track of total number of calls to Add
}

// NewBulkInsert initializes a new BulkInsert, specifying the table, columns and optional suffix
// to use.
func NewBulkInsert(ctx context.Context, db *Connection, table, columns, suffix string, txName string) (bi *BulkInsert, err error) {
	if table == "" {
		return nil, e.N(ECode090701, "a table must be specified")
	}

	if columns == "" {
		return nil, e.N(ECode090702, "at least one column must be specified")
	}

	bi = &BulkInsert{
		db:                db,
		Table:             table,
		Columns:           columns,
		Suffix:            suffix,
		maxParamPerInsert: DefaultMaxParamPerInsert,
		paramPerStatement: len(strings.Split(columns, ",")),
		mutex:             sync.RWMutex{},
		batch:             &pgx.Batch{},
	}

	// Initialize the builder
	bi.begin(ctx)

	return bi, nil
}

// SetMaxParamPerInsert sets the max param per insert. If this value is greater than the absolute
// maximum, then it will silently set it to the absolute maximum instead
func (bi *BulkInsert) SetMaxParamPerInsert(i int) {
	if i > AbsoluteMaxParamPerInsert {
		i = AbsoluteMaxParamPerInsert
	}

	bi.maxParamPerInsert = i
}

// SetMaxRowPerInsert sets the max rows per insert. If the specified number of rows
// makes the parameters per insert exceed the absolute max, then the max rows will be
// decremented until it falls into the allowed range. The number of parameters is
// equal to the maxRows multiplied by the params per statement (number of columns
// in the insert)
func (bi *BulkInsert) SetMaxRowPerInsert(maxRows uint) (actualMaxRows uint) {
	for {
		if int(maxRows)*bi.paramPerStatement > AbsoluteMaxParamPerInsert {
			maxRows--
			if maxRows == 0 {
				bi.SetMaxParamPerInsert(0)
				return 0
			}
			continue
		}
		bi.SetMaxParamPerInsert(int(maxRows) * bi.paramPerStatement)
		break
	}

	return maxRows
}

// GetMaxRowPerInsert gets the current max rows per insert (maximum params per
// insert divided by the params per statement).
func (bi *BulkInsert) GetMaxRowPerInsert() (maxRows uint) {
	return uint(bi.maxParamPerInsert / bi.paramPerStatement)
}

// GetColumnCount returns the number of columns in the bulk insert
func (bi *BulkInsert) GetColumnCount() (colCount int) {
	return bi.paramPerStatement
}

// SetPreInsert sets the pre insert func, called right before an insert is executed
func (bi *BulkInsert) SetPreInsert(f func() error) {
	bi.preInsert = f
}

// SetPreInsert sets the pre insert func, called right before an insert is executed
func (bi *BulkInsert) SetPostInsert(f func(rowCount int) error) {
	bi.postInsert = f
}

// GetCount returns the number of rows that have been added to the bulk insert
func (bi *BulkInsert) GetCount() (count int) {
	return bi.count
}

// GetTotal returns the total number of rows that have been added to the bulk insert
// since it was initialized
func (bi *BulkInsert) GetTotal() (total int) {
	bi.mutex.RLock()
	defer func() {
		bi.mutex.RUnlock()
	}()
	return bi.total
}

// Add adds the values to be sent as a bulk insert. If the number of parameters
// exceeds the max param per insert, then it will run the current build statement
// and then reset itself for more values to be added. If it executed a statement,
// it will return the current count as the number of rows inserted. This will not
// track actual rows inserted, e.g. if duplicates are ignored.
func (bi *BulkInsert) Add(ctx context.Context, values ...interface{}) (rowsInserted int, err error) {
	bi.mutex.Lock()
	defer func() {
		bi.mutex.Unlock()
	}()

	bi.count++
	bi.total++

	ib := bi.db.Insert(bi.Table).Columns(bi.Columns).Values(values...)

	if bi.Suffix != "" {
		ib = ib.Suffix(bi.Suffix)
	}

	query, bindParams, err := ib.ToSql()
	if err != nil {
		return 0, e.W(err, ECode090707)
	}

	bi.batch.Queue(query, bindParams)

	if len(values) != bi.paramPerStatement {
		return 0, e.N(ECode09070A, "number of values must equal number of columns")
	}

	// Increment the param count
	bi.paramCount += bi.paramPerStatement

	// If the param count exceeds the max param per insert, then run the query now
	if bi.paramCount > bi.maxParamPerInsert {
		// Run the currently stored statement
		if err := bi.exec(ctx); err != nil {
			return 0, e.W(err, ECode090703)
		}

		// Get the number of rows that were inserted (should be the current count)
		// Ensure this is done before the begin call, as that will reset the count
		rowsInserted = bi.count

		// Reset the param count and insert builder
		bi.begin(ctx)
	}

	return rowsInserted, nil
}

// Close the batch
func (bi *BulkInsert) Close() (errList []error) {
	if bi.batch == nil {
		return nil
	}

	bi.mutex.Lock()
	defer func() {
		bi.mutex.Unlock()
	}()

	bi.batch = &pgx.Batch{}

	return errList
}

// Flush if there is a remaining statement to run, it will
// execute the query
func (bi *BulkInsert) Flush(ctx context.Context) (err error) {
	bi.mutex.Lock()
	defer func() {
		bi.mutex.Unlock()
	}()

	if bi.paramCount > 0 {
		if err := bi.exec(ctx); err != nil {
			return e.W(err, ECode090704)
		}
	}

	bi.begin(ctx)

	return nil
}

// begin initializes an insert builder and also resets it after a statement
// has been executed
func (bi *BulkInsert) begin(ctx context.Context) {
	bi.paramCount = 0
	bi.count = 0

	bi.Close()
}

// exec runs the insert statement
func (bi *BulkInsert) exec(ctx context.Context) (err error) {
	// If preInsert is set, call it
	if bi.preInsert != nil {
		if err := bi.preInsert(); err != nil {
			return e.W(err, ECode090706)
		}
	}

	// Begin a transaction
	tx, err := bi.db.DB.Begin(ctx)
	if err != nil {
		return e.W(err, ECode090708, "error starting transaction")
	}
	defer tx.Rollback(ctx)

	count := bi.batch.Len()

	// Send the batch
	results := tx.SendBatch(ctx, bi.batch)
	defer results.Close()

	// Check for errors in the results
	for i := 0; i < count; i++ {
		_, err := results.Exec()
		if err != nil {
			msg := fmt.Sprintf("error executing batch command %d: %w", i, err)

			return e.N(ECode090709, msg)
		}
	}

	// Commit the transaction
	if err = tx.Commit(ctx); err != nil {
		return e.W(err, ECode090705, "error committing transaction")
	}

	// If post insert is set, call it
	if bi.postInsert != nil {
		if err := bi.postInsert(bi.count); err != nil {
			return e.W(err, ECode09070B)
		}
	}
	return nil
}
