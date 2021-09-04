package arcerrors

const (
	UnknownInternalServerError = "ARC.01: Unknown Internal Server Error"
	Unauthorized               = "ARC.02: Unauthorized"
	Unauthenticated            = "ARC.03: Authentication Failed"
	ErrCartCustomerExists      = "ARC.04: Cart customer already exist"
	ErrCartCustomerNotExists   = "ARC.05: Cart customer does not exist"
	ErrGrantDoesNotExist       = "ARC.06: Grant does not exist"
	ErrCredentialDoesNotExist  = "ARC.07: Credential does not exist"
	ErrInvalidGrant            = "ARC.08: invalid_grant"
	ErrArcimedesUserExists     = "ARC.09: Arcimedes user already exists"
	ErrArcimedesUserNotExists  = "ARC.0A: Arcimedes user does not exist"
)
