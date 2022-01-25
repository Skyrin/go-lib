package arc

// These constants refer to some possible error codes that come from an
// arc deployment API call (not within this library)
const (
	E01FAAE_UserAlreadyExists   = "E01FAAE"
	E01F1A8_AuthorizationFailed = "E01F1A8" // Not logged in error
	E01FWA8_InvalidGrant        = "E01FWA8" // Invalid grant error
	E01FAAP_InvalidGrantLogin   = "E01FAAP" // Invalid user for oauth2.Grant.login
)
