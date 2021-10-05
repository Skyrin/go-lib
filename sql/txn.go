package sql

import (
	"database/sql"
	"fmt"

	// Including postgres library for SQL connections
	"github.com/Skyrin/go-lib/e"
	_ "github.com/lib/pq"
)

// Txn wrapper of the *sql.Txn
type Txn struct {
	txn *sql.Tx
	// TODO: support nested transactions
}

// RollbackIfInTxn same as Rollback, except if it is in a txn, it will not
// return an error
func (t *Txn) RollbackIfInTxn() {
	if t.txn == nil {
		return
	}

	t.Rollback()
}

// Rollback attempts to roll back the txn
func (t *Txn) Rollback() (err error) {
	if t.txn == nil {
		return e.Wrap(err, e.Code020I, "01")
	}

	if err := t.txn.Rollback(); err != nil {
		return e.Wrap(err, e.Code020I, "02")
	}

	t.txn = nil

	return nil
}

// Commit attempts to commit the txn
func (t *Txn) Commit() (err error) {
	if t.txn == nil {
		return e.Wrap(err, e.Code020J, "01")
	}

	if err = t.txn.Commit(); err != nil {
		return e.Wrap(err, e.Code020J, "02")
	}

	t.txn = nil

	return nil
}

// Exec executes the query in the txn
func (t *Txn) Exec(query string, args ...interface{}) (res sql.Result, err error) {
	res, err = t.txn.Exec(query, args...)
	if err != nil {
		// Not logging args because it may contain sensitive information. The
		// caller can log them if needed
		return nil, e.Wrap(err, e.Code020K, "01", fmt.Sprintf("query: %s\n", query))
	}

	return res, nil
}

// Prepare prepares the query in the txn
func (t *Txn) Prepare(query string) (stmt *sql.Stmt, err error) {
	stmt, err = t.txn.Prepare(query)
	if err != nil {
		return nil, e.Wrap(err, e.Code020L, "01", query)
	}

	return stmt, nil
}

// Query runs the query in the txn
func (t *Txn) Query(query string, args ...interface{}) (rows *Rows, err error) {
	sqlRows, err := t.txn.Query(query, args...)
	if err != nil {
		// Not logging args because it may contain sensitive information. The
		// caller can log them if needed
		return nil, e.Wrap(err, e.Code020M, "01", fmt.Sprintf("query: %s\n", query))
	}

	return &Rows{
		rows:  sqlRows,
		query: query,
	}, nil
}

// QueryRow runs the query in the txn, returning the single row
func (t *Txn) QueryRow(query string, args ...interface{}) (row *Row) {
	return &Row{
		row:   t.txn.QueryRow(query, args...),
		query: query,
	}
}

// Stmt prepares the statement in the txn
func (t *Txn) Stmt(stmt *sql.Stmt) *sql.Stmt {
	return t.txn.Stmt(stmt)
}
