package sqlmodel

import (
	"context"
	"fmt"
	"strings"

	"github.com/Skyrin/go-lib/e"
	"github.com/Skyrin/go-lib/migrationpgx/model"
	sql "github.com/Skyrin/go-lib/sqlpgx"
)

const (
	MigrationTableName     = "skyrin_migration"
	MigrationDefaultSortBy = "skyrin_migration_id"

	ECode010301 = e.Code0103 + "01"
	ECode010302 = e.Code0103 + "02"
	ECode010303 = e.Code0103 + "03"
	ECode010304 = e.Code0103 + "04"
	ECode010305 = e.Code0103 + "05"
	ECode010306 = e.Code0103 + "06"
	ECode010307 = e.Code0103 + "07"
	ECode010308 = e.Code0103 + "08"
	ECode010309 = e.Code0103 + "09"
	ECode01030A = e.Code0103 + "0A"
	ECode01030B = e.Code0103 + "0B"
)

// MigrationGetParam get params
type MigrationGetParam struct {
	Limit          uint64
	Offset         uint64
	ID             *int
	Version        *int
	Code           *string
	Status         *string
	FlagCount      bool
	OrderByID      string
	OrderByVersion string
}

// MigrationUpdateParam update params
type MigrationUpdateParam struct {
	Version *string
	Status  *string
	SQL     *string
	Err     *string
}

// MigrationInsertParam insert params
type MigrationInsertParam struct {
	Code    string
	Version int
	Status  string
	SQL     string
	Err     string
}

// MigrationInsert performs insert
func MigrationInsert(ctx context.Context, db *sql.Connection, ip *MigrationInsertParam) (id int, err error) {
	ib := db.Insert(MigrationTableName).
		Columns(`skyrin_migration_code,skyrin_migration_version,
		skyrin_migration_status,skyrin_migration_sql,skyrin_migration_err,
		created_on,updated_on`).
		Values(ip.Code, ip.Version,
			ip.Status, ip.SQL, ip.Err,
			"now()", "now()",
		).Suffix("RETURNING skyrin_migration_id")

	id, err = db.ExecInsertReturningID(ctx, ib)
	if err != nil {
		return 0, e.W(err, ECode010301,
			fmt.Sprintf("params: %s, %v, %s, SQL redacted, %s",
				ip.Code, ip.Version, ip.Status, ip.Err))
	}

	return id, nil
}

// MigrationUpdate performs update
func MigrationUpdate(ctx context.Context, db *sql.Connection, id int, up *MigrationUpdateParam) (err error) {
	ub := db.Update(MigrationTableName).
		Set("updated_on", "now()").
		Where("skyrin_migration_id=?", id)

	if up == nil {
		return nil // Nothing to update
	}

	if up.Version != nil {
		ub = ub.Set("skyrin_migration_version", *up.Version)
	}

	if up.Status != nil {
		ub = ub.Set("skyrin_migration_status", *up.Status)
	}

	if up.SQL != nil {
		ub = ub.Set("skyrin_migration_sql", *up.SQL)
	}

	if up.Err != nil {
		ub = ub.Set("skyrin_migration_err", *up.Err)
	}

	err = db.ExecUpdate(ctx, ub)
	if err != nil {
		return e.W(err, ECode010302,
			fmt.Sprintf("params: %d, %v, %v, SQL redacted, %v",
				id, up.Version, up.Status, up.Err))
	}

	return nil
}

// MigrationGet performs select
func MigrationGet(ctx context.Context, db *sql.Connection,
	p *MigrationGetParam) (mList []*model.Migration, count int, err error) {
	if p.Limit == 0 {
		p.Limit = 1
	}

	fields := `skyrin_migration_id,skyrin_migration_code,skyrin_migration_version,
	skyrin_migration_status,skyrin_migration_sql,skyrin_migration_err,
	created_on::TEXT,updated_on::TEXT`

	sb := db.Select("{fields}").
		From(MigrationTableName).
		Limit(p.Limit)

	if p.ID != nil && *p.ID >= 0 {
		sb = sb.Where("skyrin_migration_id=?", *p.ID)
	}

	if p.Version != nil && *p.Version >= 0 {
		sb = sb.Where("skyrin_migration_version=?", *p.Version)
	}

	if p.Code != nil {
		sb = sb.Where("skyrin_migration_code=?", *p.Code)
	}

	if p.Status != nil {
		sb = sb.Where("skyrin_migration_status=?", *p.Status)
	}

	stmt, bindList, err := sb.ToSql()
	if err != nil {
		return nil, 0, e.W(err, ECode010303)
	}

	if p.FlagCount {
		row := db.QueryRow(ctx, strings.Replace(stmt, "{fields}", "count(*)", 1), bindList...)
		if err := row.Scan(&count); err != nil {
			return nil, 0, e.W(err, ECode010304,
				fmt.Sprintf("stmt: %s | bindList: %v", stmt, bindList))
		}
	}

	sb = sb.Offset(uint64(p.Offset))

	if p.OrderByID != "" {
		sb = sb.OrderBy(fmt.Sprintf("skyrin_migration_id %s", p.OrderByID))
	}

	if p.OrderByVersion != "" {
		sb = sb.OrderBy(fmt.Sprintf("skyrin_migration_version %s", p.OrderByVersion))
	}

	stmt, bindList, err = sb.ToSql()
	stmt = strings.Replace(stmt, "{fields}", fields, 1)
	rows, err := db.Query(ctx, stmt, bindList...)
	if err != nil {
		return nil, 0, e.W(err, ECode010305,
			fmt.Sprintf("bindList: %v", bindList))
	}
	defer rows.Close()

	for rows.Next() {
		m := &model.Migration{}
		if err := rows.Scan(&m.ID, &m.Code, &m.Version,
			&m.Status, &m.SQL, &m.Err,
			&m.CreatedOn, &m.UpdatedOn); err != nil {
			return nil, 0, e.W(err, ECode010306,
				fmt.Sprintf("stmt: %s | bindList: %v", stmt, bindList))
		}

		mList = append(mList, m)
	}

	if err := rows.Err(); err != nil {
		return nil, 0, fmt.Errorf("error iterating rows: %w", err)
	}

	return mList, count, nil
}

// MigrationGetByCodeAndVersion returns the migration by code and version
func MigrationGetByCodeAndVersion(ctx context.Context, db *sql.Connection, code string,
	version int) (m *model.Migration, err error) {
	mList, _, err := MigrationGet(ctx, db, &MigrationGetParam{
		Limit:   1,
		Code:    &code,
		Version: &version,
	})
	if err != nil {
		return nil, e.W(err, ECode010307)
	}

	if len(mList) != 1 {
		return nil, e.N(ECode010308, e.MsgMigrationCodeVersionDNE)
	}

	return mList[0], nil
}

// MigrationGetLatest retrieves the latest migration
func MigrationGetLatest(ctx context.Context, db *sql.Connection, code string) (m *model.Migration, err error) {
	mList, _, err := MigrationGet(ctx, db, &MigrationGetParam{
		Limit:          1,
		Code:           &code,
		OrderByVersion: "desc",
	})
	if err != nil {
		if e.ContainsError(err, e.PQErr42P01) {
			return nil, e.N(ECode010309, e.MsgMigrationNotInstalled)
		}

		return nil, e.W(err, ECode01030A)
	}

	if len(mList) != 1 {
		return nil, e.N(ECode01030B, e.MsgMigrationNone)
	}

	return mList[0], nil
}
