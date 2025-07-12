package sqlpgx

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"

	sq "github.com/Masterminds/squirrel"
	"github.com/Skyrin/go-lib/e"
	"github.com/rs/zerolog/log"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	ECode090301 = e.Code0903 + "01"
	ECode090302 = e.Code0903 + "02"
	ECode090303 = e.Code0903 + "03"
	ECode090304 = e.Code0903 + "04"
	ECode090305 = e.Code0903 + "05"
	ECode090306 = e.Code0903 + "06"
	ECode090307 = e.Code0903 + "07"
	ECode090308 = e.Code0903 + "08"
	ECode090309 = e.Code0903 + "09"
	ECode09030A = e.Code0903 + "0A"
	ECode09030B = e.Code0903 + "0B"
	ECode09030C = e.Code0903 + "0C"
	ECode09030D = e.Code0903 + "0D"
	ECode09030E = e.Code0903 + "0E"
	ECode09030F = e.Code0903 + "0F"
	ECode09030G = e.Code0903 + "0G"
	ECode09030H = e.Code0903 + "0H"
	ECode09030I = e.Code0903 + "0I"
	ECode09030J = e.Code0903 + "0J"
	ECode09030K = e.Code0903 + "0K"
	ECode09030L = e.Code0903 + "0L"
	ECode09030M = e.Code0903 + "0M"
	ECode09030N = e.Code0903 + "0N"
	ECode09030O = e.Code0903 + "0O"
	ECode09030P = e.Code0903 + "0P"
	ECode09030Q = e.Code0903 + "0Q"
	ECode09030R = e.Code0903 + "0R"
	ECode09030S = e.Code0903 + "0S"
	ECode09030T = e.Code0903 + "0T"
	ECode09030U = e.Code0903 + "0U"
	ECode09030V = e.Code0903 + "0V"
	ECode09030W = e.Code0903 + "0W"
	ECode09030X = e.Code0903 + "0X"
	ECode09030Y = e.Code0903 + "0Y"
	ECode09030Z = e.Code0903 + "0Z"
)

// Connection wrapper of the *pgxpool.Pool
type Connection struct {
	DB           *pgxpool.Pool
	Slug         *Slug
	txn          *Txn
	txnIdx       int
	statusMap    map[string][]*Status                    // Cache of statuses
	statusLoader func(db *Connection) ([]*Status, error) // Status loader
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

var (
	dbConn *Connection
	once   sync.Once
)

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
		return nil, e.W(err, ECode090301, err.Error(), configPath)
	}

	if err := json.Unmarshal(b, cp); err != nil {
		return nil, e.W(err, ECode090302, err.Error())
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
func NewPostgresConn(ctx context.Context, cp *ConnParam) (conn *Connection, err error) {
	if cp == nil {
		cp = GetConnParamFromENV()
	}

	var intError error

	config, err := pgxpool.ParseConfig(GetConnectionStr(cp))
	if err != nil {
		return nil, err
	}

	config.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
		_, err := conn.Exec(ctx, `SET TIME ZONE 'UTC'`)
		if err != nil {
			return fmt.Errorf("failed to set time zone: %w", err)
		}
		return nil
	}

	once.Do(func() {
		db, err := pgxpool.NewWithConfig(ctx, config)
		if err != nil {
			intError = e.WWM(err, ECode090303, "unable to create connection pool")
			return
		}

		if err := db.Ping(ctx); err != nil {
			intError = e.WWM(err, ECode090304, "failed to ping DB")
			return
		}

		dbConn = &Connection{DB: db}
	})

	if intError != nil {
		return nil, intError
	}

	dbConn.Slug = NewSlug(nil)

	return dbConn, nil
}

// Ping wrapper for ping
func (c *Connection) Ping(ctx context.Context) error {
	return c.DB.Ping(ctx)
}

// Close wrapper for close
func (c *Connection) Close() {
	c.DB.Close()
}

// Txn returns the underlying transaction, if currently in one
func (c *Connection) Txn() pgx.Tx {
	if c.txn != nil {
		return c.txn.txn
	}

	return nil
}

// BeginUseDefaultTxn begins a txn, storing it in the txn property
// If txn is not nil (already in a txn), it will return an error
func (c *Connection) BeginUseDefaultTxn(ctx context.Context) (err error) {
	if c.txn != nil {
		return e.W(nil, ECode090305)
	}

	txn, err := c.DB.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return e.W(err, ECode090306)
	}

	c.txn = &Txn{
		txn: txn,
	}

	return nil
}

// BeginReturnDB begins a new transaction, returning a copy of
// the database connection with the txn already set. This copy
// should be used to call all txn commands and then discarded.
func (c *Connection) BeginReturnDB(ctx context.Context) (db *Connection, err error) {
	txn, err := c.DB.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, e.W(err, ECode090307)
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
func (c *Connection) Begin(ctx context.Context) (err error) {
	if c.txn != nil {
		return e.WWM(nil, ECode090308, "in a txn")
	}
	txn, err := c.DB.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return e.W(err, ECode090309)
	}

	c.txn = &Txn{
		txn: txn,
	}

	return nil
}

// Commit wrapper for sql.Commit. If successfull, will unset the txn object
func (c *Connection) Commit(ctx context.Context) (err error) {
	if c.txn == nil {
		return e.WWM(nil, ECode09030A, "not in a txn")
	}

	txn := c.Txn()
	if err = txn.Commit(ctx); err != nil {
		return e.W(err, ECode09030B)
	}

	c.txn = nil

	return nil
}

// RollbackIfInTxn same as Rollback, except if it is in a txn, it will not
// return an error
func (c *Connection) RollbackIfInTxn(ctx context.Context) {
	if c.txn == nil {
		return
	}

	c.Rollback(ctx)
}

// Rollback wrapper for sql.Rollback - no matter what the transaction will
// be cancelled. So, we will log errors here, but will always assume the
// txn is rolled back and now unavailable
func (c *Connection) Rollback(ctx context.Context) {
	if c.txn == nil {
		log.Warn().Msg("[Connection.Rollback.1] not in txn")
		return
		// TODO: replace with this (Rollback needs to return an error)
		// return e.W(nil, "Connection.Rollback.1 - not in txn", "")
	}

	if err := c.Txn().Rollback(ctx); err != nil {
		log.Error().Err(err).Msg("[Connection.Rollback.2]")
		return
		// TODO: replace with this (Rollback needs to return an error)
		// return e.W(err, "Connection.Rollback.2", "")
	}

	c.txn = nil
}

// Query wrapper for sql.Query with automatic txn handling
func (c *Connection) Query(ctx context.Context, query string, args ...interface{}) (rows *Rows, err error) {
	if c.txn != nil {
		rows, err := c.Txn().Query(ctx, query, args...)
		if err != nil {
			// Query will be logged in: func (t *Txn) Query
			return nil, e.W(err, ECode09030C)
		}

		return &Rows{
			rows:  &rows,
			query: query,
		}, nil
	}

	sqlRows, err := c.DB.Query(ctx, query, args...)
	if err != nil {
		// Not logging args because it may contain sensitive information. The
		// caller can log them if needed
		return nil, e.W(err, ECode09030D, fmt.Sprintf("query: %s\n", query))
	}

	return &Rows{
		rows:  &sqlRows,
		query: query,
	}, nil
}

// Exec wrapper for sql.Exec with automatic txn handling
func (c *Connection) Exec(ctx context.Context, query string, args ...interface{}) (res *pgconn.CommandTag, err error) {
	if c.txn != nil {
		resConn, err := c.Txn().Exec(ctx, query, args...)
		return &resConn, err
	}

	resConn, err := c.DB.Exec(ctx, query, args...)
	if err != nil {
		// Not logging args because it may contain sensitive information. The
		// caller can log them if needed
		return nil, e.W(err, ECode09030E, fmt.Sprintf("query: %s\n", query))
	}

	return &resConn, nil
}

// QueryRow wrapper for sql.QueryRow with automatic txn handling
func (c *Connection) QueryRow(ctx context.Context, query string, args ...interface{}) (rows *Row) {
	if c.txn != nil {
		resRow := c.Txn().QueryRow(ctx, query, args...)
		return &Row{
			row:   &resRow,
			query: query,
		}
	}

	resRow := c.DB.QueryRow(ctx, query, args...)
	return &Row{
		row:   &resRow,
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
func (c *Connection) Expr(sql string, args ...interface{}) sq.Sqlizer {
	return sq.Expr(sql, args...)
}

// ToSQLAndQuery converts the select build to a SQL statement and bind parameters,
// then attempts to execute the query, returning the rows
func (c *Connection) ToSQLAndQuery(ctx context.Context, sb sq.SelectBuilder) (rows *Rows, err error) {
	stmt, bindList, err := sb.ToSql()
	if err != nil {
		// Not logging args because it may contain sensitive information. The
		// caller can log them if needed
		return nil, e.W(err, ECode09030F, fmt.Sprintf("stmt: %s\n", stmt))
	}

	if c.txn != nil {
		resRow, err := c.Txn().Query(ctx, stmt, bindList...)
		if err != nil {
			// Not logging args because it may contain sensitive information. The
			// caller can log them if needed
			return nil, e.W(err, ECode09030W, fmt.Sprintf("stmt: %s\n", stmt))
		}

		return &Rows{
			rows:  &resRow,
			query: stmt,
		}, nil
	}

	sqlRows, err := c.DB.Query(ctx, stmt, bindList...)
	if err != nil {
		// Not logging args because it may contain sensitive information. The
		// caller can log them if needed
		return nil, e.W(err, ECode09030G, fmt.Sprintf("stmt: %s\n", stmt))
	}

	return &Rows{
		rows:  &sqlRows,
		query: stmt,
	}, nil
}

// ToSQLAndQueryRow converts the select builder to a SQL statement and bind parameters,
// then attempts to execute the query, returning a single row
func (c *Connection) ToSQLAndQueryRow(ctx context.Context, sb sq.SelectBuilder) (row *Row, err error) {
	stmt, bindList, err := sb.ToSql()
	if err != nil {
		// Not logging args because it may contain sensitive information. The
		// caller can log them if needed
		return nil, e.W(err, ECode09030H, fmt.Sprintf("stmt: %s\n", stmt))
	}

	if c.txn != nil {
		resRow := c.Txn().QueryRow(ctx, stmt, bindList...)

		return &Row{
			row:   &resRow,
			query: stmt,
		}, nil
	}

	return c.QueryRow(ctx, stmt, bindList...), nil
}

// ExecInsert wrapper to generate SQL/bind list and then execute insert query
func (c *Connection) ExecInsert(ctx context.Context, ib sq.InsertBuilder) (err error) {
	stmt, bindList, err := ib.ToSql()
	if err != nil {
		// Not logging args because it may contain sensitive information. The
		// caller can log them if needed
		return e.W(err, ECode09030I, fmt.Sprintf("stmt: %s\n", stmt))
	}

	if c.txn != nil {
		if _, err := c.Txn().Exec(ctx, stmt, bindList...); err != nil {
			return e.W(err, ECode09030X)
		}

		return nil
	}

	if _, err := c.Exec(ctx, stmt, bindList...); err != nil {
		// Not logging args because it may contain sensitive information. The
		// caller can log them if needed
		return e.W(err, ECode09030J)
	}

	return nil
}

// ExecUpdate wrapper to generate SQL/bind list and then execute update query
func (c *Connection) ExecUpdate(ctx context.Context, ub sq.UpdateBuilder) (err error) {
	stmt, bindList, err := ub.ToSql()
	if err != nil {
		// Not logging args because it may contain sensitive information. The
		// caller can log them if needed
		return e.W(err, ECode09030K, fmt.Sprintf("stmt: %s\n", stmt))
	}

	if c.txn != nil {
		if _, err := c.Txn().Exec(ctx, stmt, bindList...); err != nil {
			return e.W(err, ECode09030V)
		}
	}

	if _, err := c.Exec(ctx, stmt, bindList...); err != nil {
		// Not logging args because it may contain sensitive information. The
		// caller can log them if needed
		return e.W(err, ECode09030L)
	}

	return nil
}

// ExecDelete wrapper to generate SQL/bind list and then execute delete query
func (c *Connection) ExecDelete(ctx context.Context, delB sq.DeleteBuilder) (err error) {
	stmt, bindList, err := delB.ToSql()
	if err != nil {
		// Not logging args because it may contain sensitive information. The
		// caller can log them if needed
		return e.W(err, ECode09030M, fmt.Sprintf("stmt: %s\n", stmt))
	}

	if c.txn != nil {
		if _, err := c.Txn().Exec(ctx, stmt, bindList...); err != nil {
			return e.W(err, ECode09030Y)
		}
	}

	if _, err := c.Exec(ctx, stmt, bindList...); err != nil {
		// Not logging args because it may contain sensitive information. The
		// caller can log them if needed
		return e.W(err, ECode09030N)
	}

	return nil
}

// ExecInsertReturningID wrapper to generate SQL/bind list and then execute insert query
func (c *Connection) ExecInsertReturningID(ctx context.Context, ib sq.InsertBuilder) (id int, err error) {
	stmt, bindList, err := ib.ToSql()
	if err != nil {
		// Not logging args because it may contain sensitive information. The
		// caller can log them if needed
		return 0, e.W(err, ECode09030O, fmt.Sprintf("stmt: %s\n", stmt))
	}

	if c.txn != nil {
		if err := c.Txn().QueryRow(ctx, stmt, bindList...).Scan(&id); err != nil {
			return 0, e.W(err, ECode09030Z)
		}

		return id, nil
	}

	if err := c.QueryRow(ctx, stmt, bindList...).Scan(&id); err != nil {
		// Not logging args because it may contain sensitive information. The
		// caller can log them if needed
		// The "query" is logged in Scan, so no need to add here
		return 0, e.W(err, ECode09030P)
	}

	return id, nil
}

// ToSQLWFieldAndQuery converts the select builder to a sql, replaces the
// fields in the statement with the passed fields (this assumes the fields
// that were used to build the select builder is the const FieldCount) and
// then attempts to query the statement
func (c *Connection) ToSQLWFieldAndQuery(ctx context.Context, sb sq.SelectBuilder, fields string) (rows *Rows, err error) {
	stmt, bindParams, err := sb.ToSql()
	if err != nil {
		return nil, e.W(err, ECode09030Q)
	}

	stmt = strings.Replace(stmt, FieldPlaceHolder, fields, 1)
	rows, err = c.Query(ctx, stmt, bindParams...)
	if err != nil {
		return nil, e.W(err, ECode09030R)
	}

	return rows, nil
}

// Prepare creates a prepared statement from the query
func (c *Connection) Prepare(ctx context.Context, query string, name string) (stmt *pgconn.StatementDescription, err error) {
	if c.txn == nil {
		return nil, fmt.Errorf("just for tx")
	}

	stmt, err = c.txn.Prepare(ctx, name, query)
	if err != nil {
		return nil, e.W(err, ECode09030S, fmt.Sprintf("query: %s", query))
	}

	return stmt, nil
}
