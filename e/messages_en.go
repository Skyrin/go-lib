package e

// This defines reusable error messages

const (
	MsgUnknownInternalServerError = "Unknown Internal Server Error"
	MsgUnauthorized               = "Unauthorized"
	MsgUnauthenticated            = "Authentication Failed"
	MsgForbidden                  = "Forbidden"

	// migrations
	MsgMigrationCodeVersionDNE         = "Migration code/version does not exist"
	MsgMigrationNotInstalled           = "Migrations library not installed"
	MsgMigrationNone                   = "No migrations exist yet"
	MsgMigrationFileNameInvalid        = "Invalid migration file name"
	MsgMigrationFileNameVersionInvalid = "Invalid migration file name version"
	MsgMigrationInstallFailed          = "Migrator installation failed"

	// arc
	MsgCartCustomerExists     = "Cart customer already exist"
	MsgCartCustomerNotExists  = "Cart customer does not exist"
	MsgGrantDoesNotExist      = "Grant does not exist"
	MsgCredentialDoesNotExist = "Credential does not exist"
	MsgInvalidGrant           = "invalid_grant"
	MsgArcimedesUserExists    = "Arcimedes user already exists"
	MsgArcimedesUserNotExists = "Arcimedes user does not exist"
	MsgCoreUserExists         = "Core user already exists"
	MsgCoreUserNotExists      = "Core user does not exist"
	MsgDeploymentDoesNotExist = "Deployment does not exist"
	MsgDataDoesNotExist       = "Data record does not exist"
)
