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
// 	MIGRATION_CODE = "arc"
// )
//
// // GetMigrationList returns this packages migration list
// func GetMigrationList() (ml *migration.List) {
// 	return migration.NewList(MIGRATION_CODE, migration.MIGRATION_PATH, migrations)
// }
package migration

import (
	"embed"

	"github.com/Skyrin/go-lib/errors"
	"github.com/Skyrin/go-lib/migration/model"
	"github.com/Skyrin/go-lib/migration/sqlmodel"
	"github.com/Skyrin/go-lib/sql"
	"github.com/rs/zerolog/log"
)

// Embed the migrations into the app, so that applications that include this
// package can run the upgrade if using this functionality

//go:embed db/migrations/*
var migrations embed.FS

const (
	MIGRATION_TABLE = "arc_migration"
	MIGRATION_PATH  = "db/migrations"
	MIGRATION_CODE  = "arc-migrations"
)

type Migrator struct {
	version    int
	db         *sql.Connection
	latest     *model.Migration
	migrations []*List
	// migrations    []*File
	schema        string
	migrationPath string
	code          string
}

// NewMigrator initializes a new migrator
func NewMigrator(db *sql.Connection) (m *Migrator, err error) {
	m = &Migrator{
		db: db,
	}

	// The migrator will always append it's own migration first
	if err := m.AddMigrationList(&List{
		code:       MIGRATION_CODE,
		path:       MIGRATION_PATH,
		migrations: migrations,
	}); err != nil {
		return nil, errors.Wrap(err, "NewMigrator.1", "")
	}

	return m, nil
}

// AddMigrationList adds a migration list to the migrator
func (m *Migrator) AddMigrationList(ml *List) (err error) {

	mm, err := sqlmodel.MigrationGetLatest(m.db, ml.code)
	if err != nil {
		// If the migrations library has not been installed or there are no
		// migrations for the specified code yet, then return a place holder
		// Otherwise, return the error now
		if !errors.ContainsError(err, model.ErrMigrationNone) &&
			!errors.ContainsError(err, model.ErrMigrationNotInstalled) {
			return errors.Wrap(err, "AddMigrationList.1", "")
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
		return errors.Wrap(err, "AddMigrationList.2", "")
	}

	m.migrations = append(m.migrations, ml)
	return nil
}

// Upgrade runs upgrades on all migration lists
func (m *Migrator) Upgrade() (err error) {
	// TODO: grab a lock on the DB so no other migrations will run (they should wait)

	for _, ml := range m.migrations {
		for _, f := range ml.files {
			if err := m.processFile(ml, f); err != nil {
				return errors.Wrap(err, "Migrator.Upgrade.1", "")
			}
		}
	}

	return nil
}

// processFile attempts to run the migration file
func (m *Migrator) processFile(ml *List, f *File) (err error) {
	var id int

	// Check if we tried to process it already
	mm, err := sqlmodel.MigrationGetByCodeAndVersion(m.db, ml.code, f.Version)
	if err != nil {
		// Return error if it is not model.ErrMigrationCodeVersionDNE
		if !errors.ContainsError(err, model.ErrMigrationCodeVersionDNE) {
			return errors.Wrap(err, "Migrator.processFile.1", "")
		}

		// If we didn't try to process it, then insert it now
		id, err = sqlmodel.MigrationInsert(m.db, &sqlmodel.MigrationInsertParam{
			Code:    ml.code,
			Version: f.Version,
			Status:  model.MIGRATION_STATUS_PENDING,
			SQL:     string(f.SQL),
			Err:     "",
		})
		if err != nil {
			return errors.Wrap(err, "Migrator.processFile.2", "")
		}
	} else {
		id = mm.ID
	}

	if mm != nil && mm.Status == model.MIGRATION_STATUS_COMPLETE {
		// If this version was already complete, then skip it
		return nil
	}

	// TODO: begin/commit/rollback?
	var status, errMsg string
	if _, err := m.db.Exec(string(f.SQL)); err != nil {
		status = model.MIGRATION_STATUS_FAILED
		errMsg = err.Error()
		if err2 := sqlmodel.MigrationUpdate(m.db, id, &sqlmodel.MigrationUpdateParam{
			Status: &status,
			Err:    &errMsg,
		}); err2 != nil {
			return errors.Wrap(err2, "Migrator.processFile.2", "")
		}
		return errors.Wrap(err, "Migrator.processFile.3", "")
	}

	status = model.MIGRATION_STATUS_COMPLETE
	errMsg = ""
	if err := sqlmodel.MigrationUpdate(m.db, id, &sqlmodel.MigrationUpdateParam{
		Status: &status,
		Err:    &errMsg,
	}); err != nil {
		return errors.Wrap(err, "Migrator.processFile.4", "")
	}

	log.Info().Msgf("successfully migrated '%s' to version: %v",
		ml.code, f.Version)

	return nil
}
