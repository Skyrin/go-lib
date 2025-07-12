package sqlpgx

import (
	"context"
	"fmt"

	"github.com/Skyrin/go-lib/e"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

const (
	ECode090501 = e.Code0905 + "01"
	ECode090502 = e.Code0905 + "02"
	ECode090503 = e.Code0905 + "03"
	ECode090504 = e.Code0905 + "04"
	ECode090505 = e.Code0905 + "05"
	ECode090506 = e.Code0905 + "06"
	ECode090507 = e.Code0905 + "07"
)

// Txn wrapper of the *sql.Txn
type Txn struct {
	txn pgx.Tx
	// TODO: support nested transactions
}

// RollbackIfInTxn same as Rollback, except if it is in a txn, it will not
// return an error
func (t *Txn) RollbackIfInTxn(ctx context.Context) {
	if t.txn == nil {
		return
	}

	t.Rollback(ctx)
}

// Rollback attempts to roll back the txn
func (t *Txn) Rollback(ctx context.Context) (err error) {
	if t.txn == nil {
		return e.W(err, ECode090501)
	}

	// txn := *t.txn
	if err := t.txn.Rollback(ctx); err != nil {
		return e.W(err, ECode090502)
	}

	t.txn = nil

	return nil
}

// Commit attempts to commit the txn
func (t *Txn) Commit(ctx context.Context) (err error) {
	if t.txn == nil {
		return e.W(err, ECode090503)
	}

	// txn := *t.txn
	if err = t.txn.Commit(ctx); err != nil {
		return e.W(err, ECode090504)
	}

	t.txn = nil

	return nil
}

// Exec executes the query in the txn
func (t *Txn) Exec(ctx context.Context, query string, args ...interface{}) (commandTag *pgconn.CommandTag, err error) {
	// txn := *t.txn

	fmt.Printf("\npgx txn nil?: %+v\ntxn: %+v\n", t.txn == nil, t.txn)

	fmt.Printf("query: %+v, args: %+v\n", query, args)

	fmt.Printf("1....\n")

	res, err := t.txn.Exec(ctx, query, args)
	if err != nil {
		// Not logging args because it may contain sensitive information. The
		// caller can log them if needed
		return nil, e.W(err, ECode090505, fmt.Sprintf("query: %s\n", query))
	}

	fmt.Printf("2....\n")

	return &res, nil
}

// Prepare prepares the query in the txn
func (t *Txn) Prepare(ctx context.Context, query string, name string) (stmt *pgconn.StatementDescription, err error) {
	// txn := *t.txn
	stmt, err = t.txn.Prepare(ctx, name, query)
	if err != nil {
		return nil, e.W(err, ECode090506, query)
	}

	return stmt, nil
}

// Query runs the query in the txn
func (t *Txn) Query(ctx context.Context, query string, args ...interface{}) (rows *Rows, err error) {
	// txn := *t.txn
	sqlRows, err := t.txn.Query(ctx, query, args...)
	if err != nil {
		// Not logging args because it may contain sensitive information. The
		// caller can log them if needed
		return nil, e.W(err, ECode090507, fmt.Sprintf("query: %s\n", query))
	}

	return &Rows{
		rows:  &sqlRows,
		query: query,
	}, nil
}

// QueryRow runs the query in the txn, returning the single row
func (t *Txn) QueryRow(ctx context.Context, query string, args ...interface{}) (row *Row) {
	// txn := *t.txn

	fmt.Printf("1....\n")

	fmt.Printf("query: %+v, args: %+v\n", query, args)

	resultRow := t.txn.QueryRow(ctx, query, args...)

	fmt.Printf("2....\n")
	return &Row{
		row:   &resultRow,
		query: query,
	}
}

// Stmt prepares the statement in the txn
func (t *Txn) Stmt(ctx context.Context, stmt string, name string) (*pgconn.StatementDescription, error) {
	// txn := *t.txn
	return t.txn.Prepare(ctx, name, stmt)
}
