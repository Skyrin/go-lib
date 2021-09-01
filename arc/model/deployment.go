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

// DeploymentGrant
type DeploymentGrant struct {
	ID                 int
	DeploymentID       int
	ArcUserID          int
	Session            string
	SessionExpiry      int
	Token              string
	TokenExpiry        int
	RefreshToken       string
	RefreshTokenExpiry int
	Deployment         *Deployment
}
