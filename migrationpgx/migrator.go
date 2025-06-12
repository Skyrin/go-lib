// Package migration provides automatic database migration capabilities
// Basic Usage sample:
//
// Errors should be handled, but ignored for example code
// migrator, _ := migration.NewMigrator(db *sql.Connection)
// _ = migrator.AddMigrationList(arc.GetMigrationList()) // See below
// _ = migrator.Upgrade()
//
// Example package that defines migrations
// var migrations embed.FS
//
// const (
//
//	MIGRATION_CODE = "arc"
//
// )
//
// // GetMigrationList returns this packages migration list
//
//	func GetMigrationList() (ml *migration.List) {
//		return migration.NewList(MIGRATION_CODE, migration.MIGRATION_PATH, migrations)
//	}
package migration

import (
	"context"
	"embed"

	"github.com/Skyrin/go-lib/e"
	"github.com/Skyrin/go-lib/migrationpgx/model"
	"github.com/Skyrin/go-lib/migrationpgx/sqlmodel"
	sql "github.com/Skyrin/go-lib/sqlpgx"
	"github.com/rs/zerolog/log"
)

// Embed the migrations into the app, so that applications that include this
// package can run the upgrade if using this functionality

//go:embed db/migrations/*.sql
var migrations embed.FS

const (
	MIGRATION_TABLE = "skyrin_migration"
	MIGRATION_PATH  = "db/migrations"
	MIGRATION_CODE  = "migration"

	ECode010101 = e.Code0101 + "01"
	ECode010102 = e.Code0101 + "02"
	ECode010103 = e.Code0101 + "03"
	ECode010104 = e.Code0101 + "04"
	ECode010105 = e.Code0101 + "05"
	ECode010106 = e.Code0101 + "06"
	ECode010107 = e.Code0101 + "07"
	ECode010108 = e.Code0101 + "08"
	ECode010109 = e.Code0101 + "09"
	ECode01010A = e.Code0101 + "0A"
	ECode01010B = e.Code0101 + "0B"
	ECode01010C = e.Code0101 + "0C"
	ECode01010D = e.Code0101 + "0D"
	ECode01010E = e.Code0101 + "0E"
	ECode01010F = e.Code0101 + "0F"
)

type Migrator struct {
	db         *sql.Connection
	latest     *model.Migration
	migrations []*List
}

// NewMigrator initializes a new migrator
func NewMigrator(ctx context.Context, db *sql.Connection) (m *Migrator, err error) {
	m = &Migrator{
		db: db,
	}

	// The migrator will always append it's own migration first
	ml := &List{
		code:       MIGRATION_CODE,
		path:       MIGRATION_PATH,
		migrations: migrations,
	}

	if err := m.AddMigrationList(ctx, ml); err != nil {
		if !e.ContainsError(err, e.MsgMigrationNotInstalled) {
			return nil, e.W(err, ECode010101)
		}
		if err := m.install(ctx, ml); err != nil {
			return nil, e.W(err, ECode010102)
		}
		// Try to add again now that the migrator is installed
		if err := m.AddMigrationList(ctx, ml); err != nil {
			return nil, e.W(err, ECode010103)
		}
	}

	return m, nil
}

// AddMigrationList adds a migration list to the migrator
func (m *Migrator) AddMigrationList(ctx context.Context, ml *List) (err error) {
	mm, err := sqlmodel.MigrationGetLatest(ctx, m.db, ml.code)
	if err != nil {
		// If the migrations library has not been installed or there are no
		// migrations for the specified code yet, then return a place holder
		// Otherwise, return the error now
		if !e.ContainsError(err, e.MsgMigrationNone) { // && !e.ContainsError(err, e.MsgMigrationDoesNotExist) {
			return e.W(err, ECode010104)
		}

		// If no migrations exist, then this is a brand new installation
		mm = &model.Migration{
			ID:        0,
			Code:      ml.code,
			Version:   0,
			Status:    model.MIGRATION_STATUS_PENDING,
			SQL:       "",
			Err:       "",
			CreatedOn: "",
			UpdatedOn: "",
		}
		m.latest = mm
		ml.new = true
	}

	ml.files, err = ml.GetLatestMigrationFiles(mm.Version)
	if err != nil {
		return e.W(err, ECode010105)
	}

	m.migrations = append(m.migrations, ml)
	return nil
}

// install installs the migrator, it will only run the first migration and should only be called
// once. NewMigrator logic handles when to call the installation.
func (m *Migrator) install(ctx context.Context, ml *List) (err error) {
	// TODO: grab a lock on the DB so no other migrations will run (they should wait)

	files, err := ml.GetLatestMigrationFiles(0)
	if err != nil {
		return e.W(err, ECode010106)
	}

	if len(files) == 0 {
		return e.N(ECode010107, e.MsgMigrationInstallFailed)
	}

	// Only run the first migration, the rest will be run via regular upgrade
	if _, err := m.db.Exec(ctx, string(files[0].SQL)); err != nil {
		return e.W(err, ECode010108)
	}

	return nil
}

// Upgrade runs upgrades on all migration lists
func (m *Migrator) Upgrade(ctx context.Context) (err error) {
	// TODO: grab a lock on the DB so no other migrations will run (they should wait)

	for _, ml := range m.migrations {
		for _, f := range ml.files {
			// Check if this file should be run or not
			id, run, err := m.checkShouldRunFile(ctx, ml, f)
			if err != nil {
				return e.W(err, ECode010109)
			}
			if !run {
				// If it shouldn't run, then skip it
				continue
			}

			if err := m.processFile(ctx, id, ml, f); err != nil {
				return e.W(err, ECode01010A)
			}
		}
	}

	return nil
}

// checkShouldRunFile verifies if the file should be processed or not. It will retrieve the
// associated migration record (code/version) from the skyrin_migration table. If it does not exist,
// then it will indicate to proceed. If the status is pending/failed it will also indicate to
// proceed. Otherwise, the status should be completed and it will indicate not to proceed.
// It will also return the id of the record.
func (m *Migrator) checkShouldRunFile(ctx context.Context, ml *List,
	f *File) (id int, shouldRun bool, err error) {
	// Check if we tried to process it already
	mm, err := sqlmodel.MigrationGetByCodeAndVersion(ctx, m.db, ml.code, f.Version)
	if err != nil {
		// Return error if it is not e.MsgMigrationCodeVersionDNE
		if !e.ContainsError(err, e.MsgMigrationCodeVersionDNE) {
			return 0, false, e.W(err, ECode01010B)
		}

		// If we didn't try to process it, then insert it now
		id, err = sqlmodel.MigrationInsert(ctx, m.db, &sqlmodel.MigrationInsertParam{
			Code:    ml.code,
			Version: f.Version,
			Status:  model.MIGRATION_STATUS_PENDING,
			SQL:     string(f.SQL),
			Err:     "",
		})
		if err != nil {
			return 0, false, e.W(err, ECode01010C)
		}
	} else {
		id = mm.ID
	}

	if mm != nil && mm.Status == model.MIGRATION_STATUS_COMPLETE {
		// If this version was already complete, then skip it
		return id, false, nil
	}

	if mm != nil && mm.Status == model.MIGRATION_STATUS_FAILED {
		// If this version failed, then resave the SQL as it may have changed
		newSQL := string(f.SQL)
		sqlmodel.MigrationUpdate(ctx, m.db, id, &sqlmodel.MigrationUpdateParam{
			SQL: &newSQL,
		})
	}

	return id, true, nil
}

// processFile attempts to run the migration file
func (m *Migrator) processFile(ctx context.Context, id int, ml *List, f *File) (err error) {
	// TODO: begin/commit/rollback?
	var status, errMsg string
	if _, err := m.db.Exec(ctx, string(f.SQL)); err != nil {
		status = model.MIGRATION_STATUS_FAILED
		errMsg = err.Error()
		if err2 := sqlmodel.MigrationUpdate(ctx, m.db, id, &sqlmodel.MigrationUpdateParam{
			Status: &status,
			Err:    &errMsg,
		}); err2 != nil {
			return e.W(err, ECode01010D)
		}
		return e.W(err, ECode01010E)
	}

	status = model.MIGRATION_STATUS_COMPLETE
	errMsg = ""
	if err := sqlmodel.MigrationUpdate(ctx, m.db, id, &sqlmodel.MigrationUpdateParam{
		Status: &status,
		Err:    &errMsg,
	}); err != nil {
		return e.W(err, ECode01010F)
	}

	log.Info().Msgf("successfully migrated '%s' to version: %v",
		ml.code, f.Version)

	return nil
}
