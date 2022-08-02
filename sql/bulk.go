package sql

import (
	dsql "database/sql"
	"strings"
	"sync"

	"github.com/Masterminds/squirrel"
	"github.com/Skyrin/go-lib/e"
)

const (
	DefaultMaxParamPerInsert  = 15000
	AbsoluteMaxParamPerInsert = 65535

	ECode020701 = e.Code0207 + "01"
	ECode020702 = e.Code0207 + "02"
	ECode020703 = e.Code0207 + "03"
	ECode020704 = e.Code0207 + "04"
	ECode020705 = e.Code0207 + "05"
	ECode020706 = e.Code0207 + "06"
	ECode020707 = e.Code0207 + "07"
	ECode020708 = e.Code0207 + "08"
	ECode020709 = e.Code0207 + "09"
	ECode02070A = e.Code0207 + "0A"
)

// BulkInsert allows for multiple inserts to be ran in a single query, speeding up
// inserts into a table.
type BulkInsert struct {
	db                *Connection
	maxParamPerInsert int                    // The maximum number of parameters to send per insert
	Table             string                 // The name of the table to bulk insert into
	Columns           string                 // The column list to bulk insert
	Suffix            string                 // Optional suffix to append to a bulk insert (e.g. ON CONFLICT DO NOTHING)
	ib                squirrel.InsertBuilder // The current insert builder
	paramCount        int                    // The current parameter count
	paramPerStatement int                    // The number of parameters per statement
	cache             map[int]*dsql.Stmt     // Stores cached statements, if enabled
	enableCache       bool                   // Indicate whether to enable cache or not
	mutex             sync.RWMutex           // Mutex for thread safe adding to bulk insert
	count             int                    // Keeps track of current number of calls to Add, since last Flush
	total             int                    // Keeps track of total number of calls to Add
}

// NewBulkInsert initializes a new BulkInsert, specifying the table, columns and optional suffix
// to use.
func NewBulkInsert(db *Connection, table, columns, suffix string) (bi *BulkInsert, err error) {
	if table == "" {
		return nil, e.N(ECode020701, "a table must be specified")
	}

	if columns == "" {
		return nil, e.N(ECode020702, "at least one column must be specified")
	}

	bi = &BulkInsert{
		db:                db,
		Table:             table,
		Columns:           columns,
		Suffix:            suffix,
		maxParamPerInsert: DefaultMaxParamPerInsert,
		paramPerStatement: len(strings.Split(columns, ",")),
		mutex:             sync.RWMutex{},
	}

	// Initialize the builder
	bi.begin()

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

// EnableCache enables caching of bulk insert statements. If used, Close must be called when finished
// to properly clean up the sql connections
func (bi *BulkInsert) EnableCache() {
	bi.enableCache = true
	bi.cache = make(map[int]*dsql.Stmt)
}

// DisableCache disables the cache and closes any open statements
func (bi *BulkInsert) DisableCache() (errList []error) {
	bi.enableCache = false
	bi.cache = nil
	return bi.Close()
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
func (bi *BulkInsert) Add(values ...interface{}) (rowsInserted int, err error) {
	bi.mutex.Lock()
	defer func() {
		bi.mutex.Unlock()
	}()

	bi.count++
	bi.total++

	// Append the values to the bind list
	bi.ib = bi.ib.Values(values...)

	if len(values) != bi.paramPerStatement {
		return 0, e.N(ECode02070A, "number of values must equal number of columns")
	}

	// Increment the param count
	bi.paramCount += bi.paramPerStatement

	// If the param count exceeds the max param per insert, then run the query now
	if bi.paramCount > bi.maxParamPerInsert {
		// Run the currently stored statement
		if err := bi.exec(); err != nil {
			return 0, e.W(err, ECode020703)
		}

		// Get the number of rows that were inserted (should be the current count)
		// Ensure this is done before the begin call, as that will reset the count
		rowsInserted = bi.count

		// Reset the param count and insert builder
		bi.begin()
	}

	return rowsInserted, nil
}

// Close if cache is enabled, then it closes all cached statements
func (bi *BulkInsert) Close() (errList []error) {
	if bi.cache == nil {
		return nil
	}
	bi.mutex.Lock()
	defer func() {
		bi.mutex.Unlock()
	}()

	for key, stmt := range bi.cache {
		if err := stmt.Close(); err != nil {
			errList = append(errList, err)
		}
		delete(bi.cache, key)
	}

	return errList
}

// Flush if there is a remaining statement to run, it will
// execute the query
func (bi *BulkInsert) Flush() (err error) {
	bi.mutex.Lock()
	defer func() {
		bi.mutex.Unlock()
	}()

	if bi.paramCount > 0 {
		if err := bi.exec(); err != nil {
			return e.W(err, ECode020704)
		}
	}

	bi.begin()
	return nil
}

// begin initializes an insert builder and also resets it after a statement
// has been executed
func (bi *BulkInsert) begin() {
	bi.ib = bi.db.Insert(bi.Table).Columns(bi.Columns)
	bi.paramCount = 0
	bi.count = 0
}

// exec runs the insert statement
func (bi *BulkInsert) exec() (err error) {
	if bi.Suffix != "" {
		bi.ib = bi.ib.Suffix(bi.Suffix)
	}

	if bi.enableCache {
		// Statements only change based on the nubmer of parameters. So, the cache is
		// keyed off of the current parameter count
		query, bindParams, err := bi.ib.ToSql()
		if err != nil {
			return e.W(err, ECode020707)
		}
		_, ok := bi.cache[bi.paramCount]
		if !ok {
			stmt, err := bi.db.Prepare(query)
			if err != nil {
				return e.W(err, ECode020708)
			}
			bi.cache[bi.paramCount] = stmt
		}

		_, err = bi.cache[bi.paramCount].Exec(bindParams...)
		if err != nil {
			return e.W(err, ECode020709)
		}
	} else {
		if err = bi.db.ExecInsert(bi.ib); err != nil {
			return e.W(err, ECode020705)
		}
	}

	return nil
}
