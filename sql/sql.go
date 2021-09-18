package sql

import (
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

// Connection wrapper of the *sql.DB
// If a transaction is started, it is stored internally in the txn and automatically
// used when making DB calls until commit/rollback is executed. If during a txn, a
// call outside of the txn is needed, the DB property can be accessed directly and
// used to make a query/exec/select call.
type Connection struct {
	DB   *sql.DB
	Slug *Slug
	txn  *sql.Tx
	// TODO: support nested transactions
}

// ConnParam connection parameters used to initialize a connection
type ConnParam struct {
	Host           string
	Port           string
	User           string
	Password       string
	DBName         string
	SSLMode        string
	SearchPath     string
	MigratePath    string
	MigrationTable string
}

// GetConnParamFromENV initializes new connection parameters and populates from ENV variables
func GetConnParamFromENV() (cp *ConnParam) {
	cp = &ConnParam{}

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
	if os.Getenv("DBMIGRATEPATH") != "" {
		cp.MigratePath = os.Getenv("DBMIGRATEPATH")
	}

	return cp
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
		return nil, e.WrapWithMsg(err, e.Code0201, "01", "Failed to connect to DB")
	}
	if err := sqlConn.Ping(); err != nil {
		return nil, e.WrapWithMsg(err, e.Code0201, "02", "Failed to ping DB")
	}

	return &Connection{DB: sqlConn, Slug: NewSlug(nil)}, nil
}

// Txn returns the underlying transaction, if currently in one
func (c *Connection) Txn() *sql.Tx {
	return c.txn
}

// Begin wrapper for sql.Begin. It doesn't return the txn object, but stores
// it internally and it will be used automatically for subsequent query/exec/select
// calls until commit/rollback is called
func (c *Connection) Begin() (err error) {
	if c.txn != nil {
		return e.WrapWithMsg(nil, e.Code0202, "01", "not in a txn")
	}
	c.txn, err = c.DB.Begin()
	if err != nil {
		return e.Wrap(err, e.Code0202, "02")
	}

	return nil
}

// Commit wrapper for sql.Commit. If successfull, will unset the txn object
func (c *Connection) Commit() (err error) {
	if c.txn == nil {
		return e.WrapWithMsg(nil, e.Code0203, "01", "not in a txn")
	}

	if err = c.txn.Commit(); err != nil {
		return e.Wrap(err, e.Code0203, "02")
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
		// return e.Wrap(nil, "Connection.Rollback.1 - not in txn", "")
	}

	if err := c.txn.Rollback(); err != nil {
		log.Error().Err(err).Msg("[Connection.Rollback.2]")
		return
		// TODO: replace with this (Rollback needs to return an error)
		// return e.Wrap(err, "Connection.Rollback.2", "")
	}

	c.txn = nil
}

// Query wrapper for sql.Query with automatic txn handling
func (c *Connection) Query(query string, args ...interface{}) (rows *Rows, err error) {
	if c.txn != nil {
		sqlRows, err := c.txn.Query(query, args...)
		if err != nil {
			// Not logging args because it may contain sensitive information. The
			// caller can log them if needed
			return nil, e.Wrap(err, e.Code0204, "01", fmt.Sprintf("query: %s\n", query))
		}
		return &Rows{
			rows: sqlRows,
			query: query,
		}, nil
	}

	sqlRows, err := c.DB.Query(query, args...)
	if err != nil {
		// Not logging args because it may contain sensitive information. The
		// caller can log them if needed
		return nil, e.Wrap(err, e.Code0204, "02", fmt.Sprintf("query: %s\n", query))
	}

	return &Rows{
		rows: sqlRows,
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
		return nil, e.Wrap(err, e.Code0205, "01", fmt.Sprintf("query: %s\n", query))
	}

	return res, nil
}

// QueryRow wrapper for sql.QueryRow with automatic txn handling
func (c *Connection) QueryRow(query string, args ...interface{}) (rows *Row) {
	if c.txn != nil {
		return &Row{
			row:   c.txn.QueryRow(query, args...),
			query: query,
		}
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
		return nil, e.Wrap(err, e.Code0206, "01", fmt.Sprintf("stmt: %s\n", stmt))
	}

	sqlRows, err := c.DB.Query(stmt, bindList...)
	if err != nil {
		// Not logging args because it may contain sensitive information. The
		// caller can log them if needed
		return nil, e.Wrap(err, e.Code0206, "02", fmt.Sprintf("stmt: %s\n", stmt))
	}

	return &Rows{
		rows: sqlRows,
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
		return nil, e.Wrap(err, e.Code0207, "01", fmt.Sprintf("stmt: %s\n", stmt))
	}

	return c.QueryRow(stmt, bindList...), nil
}

// ExecInsert wrapper to generate SQL/bind list and then execute insert query
func (c *Connection) ExecInsert(ib sq.InsertBuilder) (err error) {
	stmt, bindList, err := ib.ToSql()
	if err != nil {
		// Not logging args because it may contain sensitive information. The
		// caller can log them if needed
		return e.Wrap(err, e.Code0208, "01", fmt.Sprintf("stmt: %s\n", stmt))
	}

	if _, err := c.Exec(stmt, bindList...); err != nil {
		// Not logging args because it may contain sensitive information. The
		// caller can log them if needed
		return e.Wrap(err, e.Code0208, "02")
	}

	return nil
}

// ExecUpdate wrapper to generate SQL/bind list and then execute update query
func (c *Connection) ExecUpdate(ub sq.UpdateBuilder) (err error) {
	stmt, bindList, err := ub.ToSql()
	if err != nil {
		// Not logging args because it may contain sensitive information. The
		// caller can log them if needed
		return e.Wrap(err, e.Code0209, "01", fmt.Sprintf("stmt: %s\n", stmt))
	}

	if _, err := c.Exec(stmt, bindList...); err != nil {
		// Not logging args because it may contain sensitive information. The
		// caller can log them if needed
		return e.Wrap(err, e.Code0209, "02")
	}

	return nil
}

// ExecDelete wrapper to generate SQL/bind list and then execute delete query
func (c *Connection) ExecDelete(delB sq.DeleteBuilder) (err error) {
	stmt, bindList, err := delB.ToSql()
	if err != nil {
		// Not logging args because it may contain sensitive information. The
		// caller can log them if needed
		return e.Wrap(err, e.Code020A, "01", fmt.Sprintf("stmt: %s\n", stmt))
	}

	if _, err := c.Exec(stmt, bindList...); err != nil {
		// Not logging args because it may contain sensitive information. The
		// caller can log them if needed
		return e.Wrap(err, e.Code020A, "02")
	}

	return nil
}

// ExecInsertReturningID wrapper to generate SQL/bind list and then execute insert query
func (c *Connection) ExecInsertReturningID(ib sq.InsertBuilder) (id int, err error) {
	stmt, bindList, err := ib.ToSql()
	if err != nil {
		// Not logging args because it may contain sensitive information. The
		// caller can log them if needed
		return 0, e.Wrap(err, e.Code020B, "01", fmt.Sprintf("stmt: %s\n", stmt))
	}

	if err := c.QueryRow(stmt, bindList...).Scan(&id); err != nil {
		// Not logging args because it may contain sensitive information. The
		// caller can log them if needed
		return 0, e.Wrap(err, e.Code020B, "02", fmt.Sprintf("stmt: %s\n", stmt))
	}

	return id, nil
}
