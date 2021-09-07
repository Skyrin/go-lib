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
	"fmt"

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
	MIGRATION_TABLE = "skyrin_migration"
	MIGRATION_PATH  = "db/migrations"
	MIGRATION_CODE  = "migration"
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
	ml := &List{
		code:       MIGRATION_CODE,
		path:       MIGRATION_PATH,
		migrations: migrations,
	}
	if err := m.AddMigrationList(ml); err != nil {
		if !errors.ContainsError(err, model.ErrMigrationNotInstalled) {
			return nil, errors.Wrap(err, "NewMigrator.1", "")
		}
		if err := m.install(ml); err != nil {
			return nil, errors.Wrap(err, "NewMigrator.2", "")
		}
		// Try to add again now that the migrator is installed
		if err := m.AddMigrationList(ml); err != nil {
			return nil, errors.Wrap(err, "NewMigrator.3", "")
		}
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
		if !errors.ContainsError(err, model.ErrMigrationNone) {
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

// install installs the migrator, it will only run the first migration and should only be called
// once. NewMigrator logic handles when to call the installation.
func (m *Migrator) install(ml *List) (err error) {
	// TODO: grab a lock on the DB so no other migrations will run (they should wait)

	files, err := ml.GetLatestMigrationFiles(0)
	if err != nil {
		return errors.Wrap(err, "AddMigrationList.1", "")
	}

	if len(files) == 0 {
		return errors.Wrap(fmt.Errorf(model.ErrMigrationInstallFailed),
			"AddMigrationList.2", "")
	}

	// Only run the first migration, the rest will be run via regular upgrade
	if _, err := m.db.Exec(string(files[0].SQL)); err != nil {
		return errors.Wrap(err, "Migrator.install.3", "")
	}

	return nil
}

// Upgrade runs upgrades on all migration lists
func (m *Migrator) Upgrade() (err error) {
	// TODO: grab a lock on the DB so no other migrations will run (they should wait)

	for _, ml := range m.migrations {
		for _, f := range ml.files {
			// Check if this file should be run or not
			id, run, err := m.checkShouldRunFile(ml, f)
			if err != nil {
				return errors.Wrap(err, "Migrator.Upgrade.1", "")
			}
			if !run {
				// If it shouldn't run, then skip it
				continue
			}

			if err := m.processFile(id, ml, f); err != nil {
				return errors.Wrap(err, "Migrator.Upgrade.2", "")
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
func (m *Migrator) checkShouldRunFile(ml *List, f *File) (id int, shouldRun bool, err error) {
	// Check if we tried to process it already
	mm, err := sqlmodel.MigrationGetByCodeAndVersion(m.db, ml.code, f.Version)
	if err != nil {
		// Return error if it is not model.ErrMigrationCodeVersionDNE
		if !errors.ContainsError(err, model.ErrMigrationCodeVersionDNE) {
			return 0, false, errors.Wrap(err, "Migrator.checkShouldRunFile.1", "")
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
			return 0, false, errors.Wrap(err, "Migrator.checkShouldRunFile.2", "")
		}
	} else {
		id = mm.ID
	}

	if mm != nil && mm.Status == model.MIGRATION_STATUS_COMPLETE {
		// If this version was already complete, then skip it
		return id, false, nil
	}

	return id, true, nil
}

// processFile attempts to run the migration file
func (m *Migrator) processFile(id int, ml *List, f *File) (err error) {

	// // Check if we tried to process it already
	// mm, err := sqlmodel.MigrationGetByCodeAndVersion(m.db, ml.code, f.Version)
	// if err != nil {
	// 	// Return error if it is not model.ErrMigrationCodeVersionDNE
	// 	if !errors.ContainsError(err, model.ErrMigrationCodeVersionDNE) {
	// 		return errors.Wrap(err, "Migrator.processFile.1", "")
	// 	}

	// 	// If we didn't try to process it, then insert it now
	// 	id, err = sqlmodel.MigrationInsert(m.db, &sqlmodel.MigrationInsertParam{
	// 		Code:    ml.code,
	// 		Version: f.Version,
	// 		Status:  model.MIGRATION_STATUS_PENDING,
	// 		SQL:     string(f.SQL),
	// 		Err:     "",
	// 	})
	// 	if err != nil {
	// 		return errors.Wrap(err, "Migrator.processFile.2", "")
	// 	}
	// } else {
	// 	id = mm.ID
	// }

	// if mm != nil && mm.Status == model.MIGRATION_STATUS_COMPLETE {
	// 	// If this version was already complete, then skip it
	// 	return nil
	// }

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
