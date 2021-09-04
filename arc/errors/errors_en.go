package arcerrors

const (
	UnknownInternalServerError = "arc.01: Unknown Internal Server Error"
	Unauthorized               = "arc.02: Unauthorized"
	Unauthenticated            = "arc.03: Authentication Failed"
	ErrCartCustomerExists      = "arc.04: Cart Customer Already Exists"
	ErrCartCustomerNotExists   = "arc.05: Cart Customer Does Not Exist"
	ErrCartStoreNotSet         = "arc.06: Cart Store Not Set"
	ErrGrantDoesNotExist       = "arc.07: Grant does not exist"
	ErrInvalidGrant            = "invalid_grant"
)
