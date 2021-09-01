package arc

import (
	"embed"
	"fmt"

	"github.com/Skyrin/go-lib/errors"
	"github.com/Skyrin/go-lib/sql"
)

// Handles upgrading the tables related to arc (deployment/authentication)

// If using this package, the arc_ namespace must be reserved

const (
	migrationsTable = "arc_schema_migrations"
	migratePath     = "db/migrations"
)

//go:embed db/migrations/*
var migrations embed.FS

// DBUpgrade runs the database upgrade - should be called on all startups to
// ensure the DB is up-to-date
func DBUpgrade(cp *sql.ConnParam, schemaName string) (err error) {
	cp.MigrationTable = migrationsTable
	cp.MigratePath = migratePath

	// if err := makeArcMigrationsDirectory(); err != nil {
	// 	return errors.Wrap(err, "DBUpgrade.1", "")
	// }

	dirList, err := migrations.ReadDir(migratePath)
	if err != nil {
		return errors.Wrap(err, "DBUpgrade.1", "")
	}

	for _, file := range dirList {
		if file.IsDir() {
			continue
		}
		fmt.Printf("File Name: %s\n", file.Name())
		// Should be a file we are looking for
		// b, err := migrations.ReadFile(strings.Join([]string{
		// 	migratePath,
		// 	file.Name(),
		// }, "/"))
		// if err != nil {
		// 	return errors.Wrap(err, "DBUpgrade.2", "")
		// }

		// fmt.Printf("File Name: %s\n%s", file.Name(), b)
		// TODO: upgrade based on this instead of a file list
		// Ensure to set search_path to match schemaName
	}

	return nil
}

// func makeArcMigrationsDirectory() (err error) {
// 	if err := os.Mkdir(migratePath, os.ModeDir); err != nil {
// 		if os.IsExist(err) {
// 			return nil
// 		}
// 	}

// 	return errors.Wrap(err, "makeArcMigrationsDirectory.1", "")
// }

// func makeArcMigrationFile(name string) (err error) {
// 	if err := os.WriteFile(migratePath+"/"+name, os.ModeDir); err != nil {
// 		if os.IsExist(err) {
// 			return nil
// 		}
// 	}

// 	return errors.Wrap(err, "makeArcMigrationsDirectory.1", "")
// }
