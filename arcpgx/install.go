package arcpgx

import (
	"embed"

	migration "github.com/Skyrin/go-lib/migrationpgx"
)

//go:embed db/migrations/*.sql
var migrations embed.FS

const (
	MIGRATION_CODE = "arc"
)

// GetMigrationList returns this packages migration list
func GetMigrationList() (ml *migration.List) {
	return migration.NewList(MIGRATION_CODE, migration.MIGRATION_PATH, migrations)
}
