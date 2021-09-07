package model

const (
	ErrUnknownInternalServer           = "SQLM.01: Unknown Internal Server Error"
	ErrUnauthorized                    = "SQLM.02: Unauthorized"
	ErrUnauthenticated                 = "SQLM.03: Authentication Failed"
	ErrMigrationCodeVersionDNE         = "SQLM.04: Migration code/version does not exist"
	ErrMigrationNotInstalled           = "SQLM.05: Migrations library not installed"
	ErrMigrationNone                   = "SQLM.06: No migrations exist yet"
	ErrMigrationFileNameInvalid        = "SQLM.07: Invalid migration file name"
	ErrMigrationFileNameVersionInvalid = "SQLM.08: Invalid migration file name version"
	ErrMigrationInstallFailed          = "SQLM.09: Migrator installation failed"
)
