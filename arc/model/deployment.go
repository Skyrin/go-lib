package model

// Deployment
type Deployment struct {
	ID                 int
	Code               string
	ManageURL          string
	APIURL             string
	Name               string
	ClientID           string
	ClientSecret       string
	Token              string
	TokenExpiry        int
	RefreshToken       string
	RefreshTokenExpiry int
	LogEventCode       string
	LogPublishKey      string
}
