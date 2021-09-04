package model

// DeploymentGrant
type DeploymentGrant struct {
	ID                 int
	DeploymentID       int
	ArcUserID          int
	CredentialID       int
	Token              string
	TokenExpiry        int
	RefreshToken       string
	RefreshTokenExpiry int
}
