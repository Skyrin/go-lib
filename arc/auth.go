package arc

// clientAuth defines the auth to use when making an API call.
// If an  accessToken is set, that will be used
// Else if a token is set then that and optionally the username will be used
// Otherwise no auth will be used
type clientAuth struct {
	username string
	token string
	accessToken string
}

