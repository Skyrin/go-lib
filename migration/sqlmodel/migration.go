package sqlmodel

import (
	"fmt"
	"strings"

	"github.com/Skyrin/go-lib/errors"
	"github.com/Skyrin/go-lib/migration/model"
	"github.com/Skyrin/go-lib/sql"
)

const (
	MigrationTableName     = "arc_migration"
	MigrationDefaultSortBy = "arc_migration_id"
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
func MigrationInsert(db *sql.Connection, ip *MigrationInsertParam) (id int, err error) {
	ib := db.Insert(MigrationTableName).
		Columns(`arc_migration_code,arc_migration_version,
		arc_migration_status,arc_migration_sql,arc_migration_err,
		created_on,updated_on`).
		Values(ip.Code, ip.Version,
			ip.Status, ip.SQL, ip.Err,
			"now()", "now()",
		).Suffix("RETURNING arc_migration_id")

	id, err = db.ExecInsertReturningID(ib)
	if err != nil {
		return 0, errors.Wrap(err, "MigrationInsert.1", "")
	}

	return id, nil
}

// MigrationUpdate performs update
func MigrationUpdate(db *sql.Connection, id int, up *MigrationUpdateParam) (err error) {
	ub := db.Update(MigrationTableName).
		Set("updated_on", "now()").
		Where("arc_migration_id=?", id)

	if up == nil {
		return nil // Nothing to update
	}

	if up.Version != nil {
		ub = ub.Set("arc_migration_version", *up.Version)
	}

	if up.Status != nil {
		ub = ub.Set("arc_migration_status", *up.Status)
	}

	if up.SQL != nil {
		ub = ub.Set("arc_migration_sql", *up.SQL)
	}

	if up.Err != nil {
		ub = ub.Set("arc_migration_err", *up.Err)
	}

	err = db.ExecUpdate(ub)
	if err != nil {
		return errors.Wrap(err, "MigrationUpdate.1", "")
	}

	return nil
}

// MigrationGet performs select
func MigrationGet(db *sql.Connection,
	p *MigrationGetParam) (mList []*model.Migration, count int, err error) {
	if p.Limit == 0 {
		p.Limit = 1
	}

	fields := `arc_migration_id,arc_migration_code,arc_migration_version,
	arc_migration_status,arc_migration_sql,arc_migration_err,
	created_on,updated_on`

	sb := db.Select("{fields}").
		From(MigrationTableName).
		Limit(p.Limit)

	if p.ID != nil && *p.ID >= 0 {
		sb = sb.Where("arc_migration_id=?", *p.ID)
	}

	if p.Version != nil && *p.Version >= 0 {
		sb = sb.Where("arc_migration_version=?", *p.Version)
	}

	if p.Code != nil {
		sb = sb.Where("arc_migration_code=?", *p.Code)
	}

	if p.Status != nil {
		sb = sb.Where("arc_migration_status=?", *p.Status)
	}

	stmt, bindList, err := sb.ToSql()
	if err != nil {
		return nil, 0, errors.Wrap(err, "MigrationGet.1", "")
	}

	if p.FlagCount {
		row := db.QueryRow(strings.Replace(stmt, "{fields}", "count(*)", 1), bindList...)
		if err := row.Scan(&count); err != nil {
			return nil, 0, errors.Wrap(err, fmt.Sprintf("MigrationGet.2 | stmt: %s, bindList: %+v",
				stmt, bindList), "")
		}
	}

	sb = sb.Offset(uint64(p.Offset))

	if p.OrderByID != "" {
		sb = sb.OrderBy(fmt.Sprintf("arc_migration_id %s", p.OrderByID))
	}

	if p.OrderByVersion != "" {
		sb = sb.OrderBy(fmt.Sprintf("arc_migration_version %s", p.OrderByVersion))
	}

	stmt, bindList, err = sb.ToSql()
	stmt = strings.Replace(stmt, "{fields}", fields, 1)

	rows, err := db.Query(stmt, bindList...)
	if err != nil {
		return nil, 0, errors.Wrap(err, "MigrationGet.3", "")
	}
	defer rows.Close()

	for rows.Next() {
		m := &model.Migration{}
		if err := rows.Scan(&m.ID, &m.Code, &m.Version,
			&m.Status, &m.SQL, &m.Err,
			&m.CreatedOn, &m.UpdatedOn); err != nil {
			return nil, 0, errors.Wrap(err, "MigrationGet.4", "")
		}

		mList = append(mList, m)
	}

	return mList, count, nil
}

// MigrationGetByCodeAndVersion returns the migration by code and version
func MigrationGetByCodeAndVersion(db *sql.Connection, code string,
	version int) (m *model.Migration, err error) {

	mList, _, err := MigrationGet(db, &MigrationGetParam{
		Limit:   1,
		Code:    &code,
		Version: &version,
	})

	if err != nil {
		return nil, errors.Wrap(err, "MigrationGetByCodeAndVersion.1", "")
	}

	if len(mList) != 1 {
		return nil, fmt.Errorf(model.ErrMigrationCodeVersionDNE)
	}

	return mList[0], nil
}

// MigrationGetLatest retrieves the latest migration
func MigrationGetLatest(db *sql.Connection, code string) (m *model.Migration, err error) {
	mList, _, err := MigrationGet(db, &MigrationGetParam{
		Limit:          1,
		Code:           &code,
		OrderByVersion: "desc",
	})
	if err != nil {
		// Check for table does not exist error
		if errors.IsPQError(err, errors.PQErr42P01) {
			return nil, fmt.Errorf(model.ErrMigrationNotInstalled)
		}
		return nil, errors.Wrap(err, "MigrationGetLatest.1", "")
	}

	if len(mList) != 1 {
		return nil, fmt.Errorf(model.ErrMigrationNone)
	}

	return mList[0], nil
}
