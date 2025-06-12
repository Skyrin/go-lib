package model

type MigrationStatus string

const (
	MIGRATION_STATUS_PENDING  = "pending"
	MIGRATION_STATUS_FAILED   = "failed"
	MIGRATION_STATUS_COMPLETE = "complete"
)

// Migration
type Migration struct {
	ID        int
	Code      string
	Version   int
	Status    MigrationStatus
	SQL       string
	Err       string
	CreatedOn string
	UpdatedOn string
}
