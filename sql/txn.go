package sql

import (
	"database/sql"
	"fmt"

	// Including postgres library for SQL connections
	"github.com/Skyrin/go-lib/e"
	_ "github.com/lib/pq"
)

const (
	ECode020501 = e.Code0205 + "01"
	ECode020502 = e.Code0205 + "02"
	ECode020503 = e.Code0205 + "03"
	ECode020504 = e.Code0205 + "04"
	ECode020505 = e.Code0205 + "05"
	ECode020506 = e.Code0205 + "06"
	ECode020507 = e.Code0205 + "07"
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
		return e.W(err, ECode020501)
	}

	if err := t.txn.Rollback(); err != nil {
		return e.W(err, ECode020502)
	}

	t.txn = nil

	return nil
}

// Commit attempts to commit the txn
func (t *Txn) Commit() (err error) {
	if t.txn == nil {
		return e.W(err, ECode020503)
	}

	if err = t.txn.Commit(); err != nil {
		return e.W(err, ECode020504)
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
		return nil, e.W(err, ECode020505, fmt.Sprintf("query: %s\n", query))
	}

	return res, nil
}

// Prepare prepares the query in the txn
func (t *Txn) Prepare(query string) (stmt *sql.Stmt, err error) {
	stmt, err = t.txn.Prepare(query)
	if err != nil {
		return nil, e.W(err, ECode020506, query)
	}

	return stmt, nil
}

// Query runs the query in the txn
func (t *Txn) Query(query string, args ...interface{}) (rows *Rows, err error) {
	sqlRows, err := t.txn.Query(query, args...)
	if err != nil {
		// Not logging args because it may contain sensitive information. The
		// caller can log them if needed
		return nil, e.W(err, ECode020507, fmt.Sprintf("query: %s\n", query))
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
