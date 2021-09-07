package arc

import (
	"embed"

	"github.com/Skyrin/go-lib/migration"
)

//go:embed db/migrations/*
var migrations embed.FS

const (
	MIGRATION_CODE = "arc"
)

// GetMigrationList returns this packages migration list
func GetMigrationList() (ml *migration.List) {
	return migration.NewList(MIGRATION_CODE, migration.MIGRATION_PATH, migrations)
}
