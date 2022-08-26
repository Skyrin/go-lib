package sql

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"database/sql"

	sq "github.com/Masterminds/squirrel"
	"github.com/Skyrin/go-lib/e"
	"github.com/rs/zerolog/log"

	// Including postgres library for SQL connections
	_ "github.com/lib/pq"
)

const (
	ECode020301 = e.Code0203 + "01"
	ECode020302 = e.Code0203 + "02"
	ECode020303 = e.Code0203 + "03"
	ECode020304 = e.Code0203 + "04"
	ECode020305 = e.Code0203 + "05"
	ECode020306 = e.Code0203 + "06"
	ECode020307 = e.Code0203 + "07"
	ECode020308 = e.Code0203 + "08"
	ECode020309 = e.Code0203 + "09"
	ECode02030A = e.Code0203 + "0A"
	ECode02030B = e.Code0203 + "0B"
	ECode02030C = e.Code0203 + "0C"
	ECode02030D = e.Code0203 + "0D"
	ECode02030E = e.Code0203 + "0E"
	ECode02030F = e.Code0203 + "0F"
	ECode02030G = e.Code0203 + "0G"
	ECode02030H = e.Code0203 + "0H"
	ECode02030I = e.Code0203 + "0I"
	ECode02030J = e.Code0203 + "0J"
	ECode02030K = e.Code0203 + "0K"
	ECode02030L = e.Code0203 + "0L"
	ECode02030M = e.Code0203 + "0M"
	ECode02030N = e.Code0203 + "0N"
	ECode02030O = e.Code0203 + "0O"
	ECode02030P = e.Code0203 + "0P"
	ECode02030Q = e.Code0203 + "0Q"
	ECode02030R = e.Code0203 + "0R"
	ECode02030S = e.Code0203 + "0S"
	ECode02030T = e.Code0203 + "0T"
	ECode02030U = e.Code0203 + "0U"
)

// Connection wrapper of the *sql.DB
// If a transaction is started, it is stored internally in the txn and automatically
// used when making DB calls until commit/rollback is executed. If during a txn, a
// call outside of the txn is needed, the DB property can be accessed directly and
// used to make a query/exec/select call.
type Connection struct {
	DB           *sql.DB
	Slug         *Slug
	txn          *Txn
	txnIdx       int
	statusMap    map[string][]*Status                    // Cache of statuses
	statusLoader func(db *Connection) ([]*Status, error) // Status loader
	// TODO: Keep a pool of Connection objects for reuse?
}

// ConnParam connection parameters used to initialize a connection
type ConnParam struct {
	Host       string `json:"host"`
	Port       string `json:"port"`
	User       string `json:"user"`
	Password   string `json:"password"`
	DBName     string `json:"dbname"`
	SSLMode    string `json:"sslmode"`
	SearchPath string `json:"searchpath"`
}

// GetConnParamFromENV initializes new connection parameters and populates from ENV variables
func GetConnParamFromENV() (cp *ConnParam) {
	cp = &ConnParam{}

	if os.Getenv("DBCONFIGPATH") != "" {
		cp, _ = GetConnParamFromJSONConfig(os.Getenv("DBCONFIGPATH"))
		return cp
	}

	if os.Getenv("DBHOST") != "" {
		cp.Host = os.Getenv("DBHOST")
	}
	if os.Getenv("DBPORT") != "" {
		cp.Port = os.Getenv("DBPORT")
	}
	if os.Getenv("DBUSER") != "" {
		cp.User = os.Getenv("DBUSER")
	}
	if os.Getenv("DBPASS") != "" {
		cp.Password = os.Getenv("DBPASS")
	}
	if os.Getenv("DBNAME") != "" {
		cp.DBName = os.Getenv("DBNAME")
	}
	if os.Getenv("SSLMODE") != "" {
		cp.SSLMode = fmt.Sprintf("sslmode=%s", os.Getenv("SSLMODE"))
	}
	if os.Getenv("DBSEARCHPATH") != "" {
		cp.SearchPath = fmt.Sprintf("search_path=%s", os.Getenv("DBSEARCHPATH"))
	}

	return cp
}

// GetConnParamFromJSONConfig get connection params from a JSON config
func GetConnParamFromJSONConfig(configPath string) (cp *ConnParam, err error) {
	cp = &ConnParam{}

	b, err := os.ReadFile(configPath)
	if err != nil {
		return nil, e.W(err, ECode020301, err.Error(), configPath)
	}

	if err := json.Unmarshal(b, cp); err != nil {
		return nil, e.W(err, ECode020302, err.Error())
	}

	if cp.SSLMode != "" {
		cp.SSLMode = fmt.Sprintf("sslmode=%s", cp.SSLMode)
	}

	if cp.SearchPath != "" {
		cp.SearchPath = fmt.Sprintf("search_path=%s", cp.SearchPath)
	}

	return cp, nil
}

// GetConnectionStr returns a connection string
func GetConnectionStr(cp *ConnParam) (connStr string) {
	var csb strings.Builder

	if cp == nil {
		cp = GetConnParamFromENV()
	}

	_, _ = csb.WriteString("host=")
	_, _ = csb.WriteString(cp.Host)
	_, _ = csb.WriteString(" port=")
	_, _ = csb.WriteString(cp.Port)
	_, _ = csb.WriteString(" user=")
	_, _ = csb.WriteString(cp.User)
	_, _ = csb.WriteString(" password=")
	_, _ = csb.WriteString(cp.Password)
	_, _ = csb.WriteString(" dbname=")
	_, _ = csb.WriteString(cp.DBName)

	_, _ = csb.WriteString(" ")
	if cp.SSLMode != "" {
		_, _ = csb.WriteString(cp.SSLMode)
	} else {
		_, _ = csb.WriteString("sslmode=require")
	}

	if cp.SearchPath != "" {
		_, _ = csb.WriteString(" ")
		_, _ = csb.WriteString(cp.SearchPath)

	}

	return csb.String()
}

// NewPostgresConn initializes a new Postgres connection
// FIXME: use a pool?
func NewPostgresConn(cp *ConnParam) (conn *Connection, err error) {
	if cp == nil {
		cp = GetConnParamFromENV()
	}

	//TODO: handle errors better
	sqlConn, err := sql.Open("postgres", GetConnectionStr(cp))
	if err != nil {
		return nil, e.WWM(err, ECode020303, "Failed to connect to DB")
	}
	if err := sqlConn.Ping(); err != nil {
		return nil, e.WWM(err, ECode020304, "Failed to ping DB")
	}

	return &Connection{DB: sqlConn, Slug: NewSlug(nil)}, nil
}

// Txn returns the underlying transaction, if currently in one
func (c *Connection) Txn() *sql.Tx {
	if c.txn != nil {
		return c.txn.txn
	}

	return nil
}

// BeginUseDefaultTxn begins a txn, storing it in the txn property
// If txn is not nil (already in a txn), it will return an error
func (c *Connection) BeginUseDefaultTxn() (err error) {
	if c.txn != nil {
		return e.W(nil, ECode020305)
	}
	txn, err := c.DB.Begin()
	if err != nil {
		return e.W(err, ECode020306)
	}

	c.txn = &Txn{
		txn: txn,
	}

	return nil
}

// BeginReturnDB begins a new transaction, returning a copy of
// the database connection with the txn already set. This copy
// should be used to call all txn commands and then discarded.
func (c *Connection) BeginReturnDB() (db *Connection, err error) {
	txn, err := c.DB.Begin()
	if err != nil {
		return nil, e.W(err, ECode020307)
	}

	c.txnIdx = c.txnIdx + 1

	return &Connection{
		DB:   c.DB,
		Slug: c.Slug,
		txn: &Txn{
			txn: txn,
		},
		txnIdx:       c.txnIdx,
		statusMap:    c.statusMap,
		statusLoader: c.statusLoader,
	}, nil
}

// Begin wrapper for sql.Begin. It doesn't return the txn object, but stores
// it internally and it will be used automatically for subsequent query/exec/select
// calls until commit/rollback is called. This is not thread safe and shouldn't be
// called within a go routine
func (c *Connection) Begin() (err error) {
	if c.txn != nil {
		return e.WWM(nil, ECode020308, "in a txn")
	}
	txn, err := c.DB.Begin()
	if err != nil {
		return e.W(err, ECode020309)
	}

	c.txn = &Txn{
		txn: txn,
	}

	return nil
}

// Commit wrapper for sql.Commit. If successfull, will unset the txn object
func (c *Connection) Commit() (err error) {
	if c.txn == nil {
		return e.WWM(nil, ECode02030A, "not in a txn")
	}

	if err = c.txn.Commit(); err != nil {
		return e.W(err, ECode02030B)
	}

	c.txn = nil

	return nil
}

// RollbackIfInTxn same as Rollback, except if it is in a txn, it will not
// return an error
func (c *Connection) RollbackIfInTxn() {
	if c.txn == nil {
		return
	}

	c.Rollback()
}

// Rollback wrapper for sql.Rollback - no matter what the transaction will
// be cancelled. So, we will log errors here, but will always assume the
// txn is rolled back and now unavailable
func (c *Connection) Rollback() {
	if c.txn == nil {
		log.Warn().Msg("[Connection.Rollback.1] not in txn")
		return
		// TODO: replace with this (Rollback needs to return an error)
		// return e.W(nil, "Connection.Rollback.1 - not in txn", "")
	}

	if err := c.txn.Rollback(); err != nil {
		log.Error().Err(err).Msg("[Connection.Rollback.2]")
		return
		// TODO: replace with this (Rollback needs to return an error)
		// return e.W(err, "Connection.Rollback.2", "")
	}

	c.txn = nil
}

// Query wrapper for sql.Query with automatic txn handling
func (c *Connection) Query(query string, args ...interface{}) (rows *Rows, err error) {
	if c.txn != nil {
		rows, err := c.txn.Query(query, args...)
		if err != nil {
			// Query will be logged in: func (t *Txn) Query
			return nil, e.W(err, ECode02030C)
		}
		return rows, nil
	}

	sqlRows, err := c.DB.Query(query, args...)
	if err != nil {
		// Not logging args because it may contain sensitive information. The
		// caller can log them if needed
		return nil, e.W(err, ECode02030D, fmt.Sprintf("query: %s\n", query))
	}

	return &Rows{
		rows:  sqlRows,
		query: query,
	}, nil
}

// Exec wrapper for sql.Exec with automatic txn handling
func (c *Connection) Exec(query string, args ...interface{}) (res sql.Result, err error) {
	if c.txn != nil {
		return c.txn.Exec(query, args...)
	}
	res, err = c.DB.Exec(query, args...)
	if err != nil {
		// Not logging args because it may contain sensitive information. The
		// caller can log them if needed
		return nil, e.W(err, ECode02030E, fmt.Sprintf("query: %s\n", query))
	}

	return res, nil
}

// QueryRow wrapper for sql.QueryRow with automatic txn handling
func (c *Connection) QueryRow(query string, args ...interface{}) (rows *Row) {
	if c.txn != nil {
		return c.txn.QueryRow(query, args...)
	}
	return &Row{
		row:   c.DB.QueryRow(query, args...),
		query: query,
	}
}

// Select wrapper for github.com/Masterminds/squirrel.Select
func (c *Connection) Select(columns ...string) sq.SelectBuilder {
	return sq.StatementBuilder.PlaceholderFormat(sq.Dollar).Select(columns...)
}

// Insert wrapper for github.com/Masterminds/squirrel.Insert
func (c *Connection) Insert(table string) sq.InsertBuilder {
	return sq.StatementBuilder.PlaceholderFormat(sq.Dollar).Insert(table)
}

// Delete wrapper for github.com/Masterminds/squirrel.Delete
func (c *Connection) Delete(from string) sq.DeleteBuilder {
	return sq.StatementBuilder.PlaceholderFormat(sq.Dollar).Delete(from)
}

// Update wrapper for github.com/Masterminds/squirrel.Update
func (c *Connection) Update(table string) sq.UpdateBuilder {
	return sq.StatementBuilder.PlaceholderFormat(sq.Dollar).Update(table)
}

// Expr wrapper for github.com/Masterminds/squirrel.Expr
func (c *Connection) Expr(sql string, args interface{}) sq.Sqlizer {
	return sq.Expr(sql, args)
}

// ToSQLAndQuery converts the select build to a SQL statement and bind parameters,
// then attempts to execute the query, returning the rows
func (c *Connection) ToSQLAndQuery(sb sq.SelectBuilder) (rows *Rows, err error) {
	stmt, bindList, err := sb.ToSql()
	if err != nil {
		// Not logging args because it may contain sensitive information. The
		// caller can log them if needed
		return nil, e.W(err, ECode02030F, fmt.Sprintf("stmt: %s\n", stmt))
	}

	sqlRows, err := c.DB.Query(stmt, bindList...)
	if err != nil {
		// Not logging args because it may contain sensitive information. The
		// caller can log them if needed
		return nil, e.W(err, ECode02030G, fmt.Sprintf("stmt: %s\n", stmt))
	}

	return &Rows{
		rows:  sqlRows,
		query: stmt,
	}, nil
}

// ToSQLAndQueryRow converts the select builder to a SQL statement and bind parameters,
// then attempts to execute the query, returning a single row
func (c *Connection) ToSQLAndQueryRow(sb sq.SelectBuilder) (row *Row, err error) {
	stmt, bindList, err := sb.ToSql()
	if err != nil {
		// Not logging args because it may contain sensitive information. The
		// caller can log them if needed
		return nil, e.W(err, ECode02030H, fmt.Sprintf("stmt: %s\n", stmt))
	}

	return c.QueryRow(stmt, bindList...), nil
}

// ExecInsert wrapper to generate SQL/bind list and then execute insert query
func (c *Connection) ExecInsert(ib sq.InsertBuilder) (err error) {
	stmt, bindList, err := ib.ToSql()
	if err != nil {
		// Not logging args because it may contain sensitive information. The
		// caller can log them if needed
		return e.W(err, ECode02030I, fmt.Sprintf("stmt: %s\n", stmt))
	}

	if _, err := c.Exec(stmt, bindList...); err != nil {
		// Not logging args because it may contain sensitive information. The
		// caller can log them if needed
		return e.W(err, ECode02030J)
	}

	return nil
}

// ExecUpdate wrapper to generate SQL/bind list and then execute update query
func (c *Connection) ExecUpdate(ub sq.UpdateBuilder) (err error) {
	stmt, bindList, err := ub.ToSql()
	if err != nil {
		// Not logging args because it may contain sensitive information. The
		// caller can log them if needed
		return e.W(err, ECode02030K, fmt.Sprintf("stmt: %s\n", stmt))
	}

	if _, err := c.Exec(stmt, bindList...); err != nil {
		// Not logging args because it may contain sensitive information. The
		// caller can log them if needed
		return e.W(err, ECode02030L)
	}

	return nil
}

// ExecDelete wrapper to generate SQL/bind list and then execute delete query
func (c *Connection) ExecDelete(delB sq.DeleteBuilder) (err error) {
	stmt, bindList, err := delB.ToSql()
	if err != nil {
		// Not logging args because it may contain sensitive information. The
		// caller can log them if needed
		return e.W(err, ECode02030M, fmt.Sprintf("stmt: %s\n", stmt))
	}

	if _, err := c.Exec(stmt, bindList...); err != nil {
		// Not logging args because it may contain sensitive information. The
		// caller can log them if needed
		return e.W(err, ECode02030N)
	}

	return nil
}

// ExecInsertReturningID wrapper to generate SQL/bind list and then execute insert query
func (c *Connection) ExecInsertReturningID(ib sq.InsertBuilder) (id int, err error) {
	stmt, bindList, err := ib.ToSql()
	if err != nil {
		// Not logging args because it may contain sensitive information. The
		// caller can log them if needed
		return 0, e.W(err, ECode02030O, fmt.Sprintf("stmt: %s\n", stmt))
	}

	if err := c.QueryRow(stmt, bindList...).Scan(&id); err != nil {
		// Not logging args because it may contain sensitive information. The
		// caller can log them if needed
		// The "query" is logged in Scan, so no need to add here
		return 0, e.W(err, ECode02030P)
	}

	return id, nil
}

// ToSQLWFieldAndQuery converts the select builder to a sql, replaces the
// fields in the statement with the passed fields (this assumes the fields
// that were used to build the select builder is the const FieldCount) and
// then attempts to query the statement
func (c *Connection) ToSQLWFieldAndQuery(sb sq.SelectBuilder, fields string) (rows *Rows, err error) {
	stmt, bindParams, err := sb.ToSql()
	if err != nil {
		return nil, e.W(err, ECode02030Q)
	}

	stmt = strings.Replace(stmt, FieldPlaceHolder, fields, 1)
	rows, err = c.Query(stmt, bindParams...)
	if err != nil {
		return nil, e.W(err, ECode02030R)
	}

	return rows, nil
}

// Prepare creates a prepared statement from the query
func (c *Connection) Prepare(query string) (stmt *sql.Stmt, err error) {
	stmt, err = c.DB.Prepare(query)
	if err != nil {
		return nil, e.W(err, ECode02030S, fmt.Sprintf("query: %s"))
	}

	return stmt, nil
}
