package model

// DeploymentGrant
type DeploymentGrant struct {
	ID                 int
	DeploymentID       int
	ArcUserID          int
	ClientID           string
	ClientSecret       string
	Token              string
	TokenExpiry        int
	RefreshToken       string
	RefreshTokenExpiry int
}
