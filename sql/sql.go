package sql

import (
	"fmt"
	"os"

	"database/sql"

	sq "github.com/Masterminds/squirrel"
	"github.com/pkg/errors"
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
	Host       string
	Port       string
	User       string
	Password   string
	DBName     string
	SSLMode    string
	SearchPath string
}

func getConnParamFromENV() (cp *ConnParam, err error) {
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

	return cp, nil
}

// NewPostgresConn initializes a new Postgres connection
// FIXME: use a pool?
func NewPostgresConn(cp *ConnParam) (conn *Connection, err error) {
	if cp == nil {
		cp, err = getConnParamFromENV()
		if err != nil {
			return nil, errors.Wrap(err, "[NewPostgresConn.1]")
		}
	}

	//TODO: handle errors better
	connStr := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s %s %s",
		cp.Host, cp.Port, cp.User, cp.Password, cp.DBName, cp.SSLMode, cp.SearchPath)
	sqlConn, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, errors.Wrap(err, "[NewPostgresConn.2]")
	}
	if err := sqlConn.Ping(); err != nil {
		return nil, errors.Wrap(err, "[NewPostgresConn.3]")
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
		return fmt.Errorf("[Connection.Begin.1] already in txn")
	}
	c.txn, err = c.DB.Begin()
	if err != nil {
		return errors.Wrap(err, "[Connection.Begin.2]")
	}

	return nil
}

// Commit wrapper for sql.Commit. If successfull, will unset the txn object
func (c *Connection) Commit() (err error) {
	if c.txn == nil {
		return fmt.Errorf("[Connection.Commit.1] not in txn")
	}

	if err = c.txn.Commit(); err != nil {
		return errors.Wrap(err, "[Connection.Commit.2]")
	}

	c.txn = nil

	return nil
}

// RollbackIfInTxn same as Rollback, except if it is in a txn, it will not
// product a warning
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
	}

	if err := c.txn.Rollback(); err != nil {
		log.Error().Err(err).Msg("[Connection.Rollback.2]")
	}

	c.txn = nil

	return
}

// Query wrapper for sql.Query with automatic txn handling
func (c *Connection) Query(query string, args ...interface{}) (rows *sql.Rows, err error) {
	if c.txn != nil {
		return c.txn.Query(query, args...)
	}

	rows, err = c.DB.Query(query, args...)
	if err != nil {
		if err != sql.ErrNoRows {
			// TODO: redact potential sensitive information in args
			log.Warn().Err(err).Msgf("query: %s\nargs: %v", query, args)
		}
	}

	return rows, err
}

// Exec wrapper for sql.Exec with automatic txn handling
func (c *Connection) Exec(query string, args ...interface{}) (res sql.Result, err error) {
	if c.txn != nil {
		return c.txn.Exec(query, args...)
	}
	res, err = c.DB.Exec(query, args...)
	if err != nil {
		// TODO: redact potential sensitive information in args
		log.Warn().Err(err).Msgf("query: %s\nargs: %v", query, args)
	}

	return res, err
}

// QueryRow wrapper for sql.QueryRow with automatic txn handling
func (c *Connection) QueryRow(query string, args ...interface{}) (rows *sql.Row) {
	if c.txn != nil {
		return c.txn.QueryRow(query, args...)
	}
	return c.DB.QueryRow(query, args...)
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
func (c *Connection) ToSQLAndQuery(sb sq.SelectBuilder) (rows *sql.Rows, err error) {
	stmt, bindList, err := sb.ToSql()
	if err != nil {
		log.Error().Err(err).Msgf("[Connection.ToSQLAndQuery.1] failed to generate select query - stmt: %s | bind: %+v",
			stmt, bindList)
		return nil, errors.Wrap(err, "[Connection.ToSQLAndQuery.1]")
	}

	rows, err = c.DB.Query(stmt, bindList...)
	if err != nil {
		log.Error().Err(err).Msgf("[Connection.ToSQLAndQuery.2] failed to run select query - stmt: %s | bind: %+v",
			stmt, bindList)
		return nil, errors.Wrap(err, "[Connection.ToSQLAndQuery.2]")
	}

	return rows, nil
}

// ToSQLAndQueryRow converts the select builder to a SQL statement and bind parameters,
// then attempts to execute the query, returning a single row
func (c *Connection) ToSQLAndQueryRow(sb sq.SelectBuilder) (row *sql.Row, err error) {
	stmt, bindList, err := sb.ToSql()
	if err != nil {
		log.Error().Err(err).Msgf("[Connection.ToSQLAndQueryRow.1] failed to generate select query - stmt: %s | bind: %+v",
			stmt, bindList)
		return nil, errors.Wrap(err, "[Connection.ToSQLAndQueryRow.1]")
	}

	return c.DB.QueryRow(stmt, bindList...), nil
}

// ExecInsert wrapper to generate SQL/bind list and then execute insert query
func (c *Connection) ExecInsert(ib sq.InsertBuilder) (err error) {
	stmt, bindList, err := ib.ToSql()
	if err != nil {
		log.Error().Err(err).
			Msgf("failed to generate insert query - stmt: %s | bind: %+v",
				stmt, bindList)
		return errors.Wrap(err, "[Connection.ExecInsert.1]")
	}

	if _, err := c.Exec(stmt, bindList...); err != nil {
		return errors.Wrap(err, "[Connection.ExecInsert.2]")
	}

	return nil
}

// ExecUpdate wrapper to generate SQL/bind list and then execute update query
func (c *Connection) ExecUpdate(ub sq.UpdateBuilder) (err error) {
	stmt, bindList, err := ub.ToSql()
	if err != nil {
		log.Error().Err(err).
			Msgf("failed to generate update query - stmt: %s | bind: %+v",
				stmt, bindList)
		return errors.Wrap(err, "[Connection.ExecUpdate.1]")
	}

	if _, err := c.Exec(stmt, bindList...); err != nil {
		return errors.Wrap(err, "[Connection.ExecUpdate.2]")
	}

	return nil
}

// ExecDelete wrapper to generate SQL/bind list and then execute delete query
func (c *Connection) ExecDelete(delB sq.DeleteBuilder) (err error) {
	stmt, bindList, err := delB.ToSql()
	if err != nil {
		log.Error().Err(err).
			Msgf("failed to generate delete query - stmt: %s | bind: %+v",
				stmt, bindList)
		return errors.Wrap(err, "[Connection.ExecDelete.1]")
	}

	if _, err := c.Exec(stmt, bindList...); err != nil {
		return errors.Wrap(err, "[Connection.ExecDelete.2]")
	}

	return nil
}

// ExecInsertReturningID wrapper to generate SQL/bind list and then execute insert query
func (c *Connection) ExecInsertReturningID(ib sq.InsertBuilder) (id int, err error) {
	stmt, bindList, err := ib.ToSql()
	if err != nil {
		log.Error().Err(err).
			Msgf("failed to generate insert query - stmt: %s | bind: %+v",
				stmt, bindList)
		return 0, errors.Wrap(err, "[Connection.ExecInsert.1]")
	}

	if err := c.QueryRow(stmt, bindList...).Scan(&id); err != nil {
		return 0, errors.Wrap(err, "[Connection.ExecInsert.2]")
	}

	return id, nil
}
