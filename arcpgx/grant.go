package arcpgx

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/Skyrin/go-lib/arcpgx/model"
	"github.com/Skyrin/go-lib/arcpgx/sqlmodel"
	"github.com/Skyrin/go-lib/e"
	sql "github.com/Skyrin/go-lib/sqlpgx"
)

const (
	ECode040Q01 = e.Code040Q + "01"
	ECode040Q02 = e.Code040Q + "02"
	ECode040Q03 = e.Code040Q + "03"
	ECode040Q04 = e.Code040Q + "04"
	ECode040Q05 = e.Code040Q + "05"
	ECode040Q06 = e.Code040Q + "06"
	ECode040Q07 = e.Code040Q + "07"
	ECode040Q08 = e.Code040Q + "08"
	ECode040Q09 = e.Code040Q + "09"
	ECode040Q0A = e.Code040Q + "0A"
	ECode040Q0B = e.Code040Q + "0B"
	ECode040Q0C = e.Code040Q + "0C"
)

// Grant
type Grant struct {
	Token              string `json:"accessToken"`
	TokenExpiry        int    `json:"tokenExpiry"`
	Scope              string `json:"scope"`
	RefreshToken       string `json:"refreshToken"`
	RefreshTokenExpiry int    `json:"refreshTokenExpiry"`
	TokenType          string `json:"tokenType"`
	ArcUserID          int    `json:"userId"`
}

// IsAboutToExpireExpire returns true if this grant's token is about to expire (within 60 seconds)
func (g *Grant) IsAboutToExpireExpire() bool {
	return g.TokenExpiry < int(time.Now().Unix())-60
}

// RefreshTokenIsAboutToExpireExpire returns true if this grant's refresh token is about to
// expire (within 60 seconds)
func (g *Grant) RefreshTokenIsAboutToExpireExpire() bool {
	return g.RefreshTokenExpiry < int(time.Now().Unix())-60
}

func SQLDeploymentGrantToGrant(dg *model.DeploymentGrant) (g *Grant) {
	return &Grant{
		Token:              dg.Token,
		TokenExpiry:        dg.TokenExpiry,
		RefreshToken:       dg.RefreshToken,
		RefreshTokenExpiry: dg.RefreshTokenExpiry,
	}
}

// grantClientCredentials get grant via client credentials
func grantClientCredentials(c *Client, id, secret string) (g *Grant, err error) {
	ri := &RequestItem{
		Service: "core",
		Action:  "oauth2.Grant.clientCredentials",
		Params: []interface{}{
			id, secret, "all",
		},
	}

	res, err := c.sendSingleRequestItem(c.deployment.getManageCoreServiceURL(), ri, nil)
	if err != nil {
		return nil, e.W(err, ECode040Q01)
	}

	if !res.Success {

		return nil, e.W(err, ECode040Q02,
			fmt.Sprintf("[%s]%s", res.ErrorCode, res.Message))
	}

	g = &Grant{}
	if err := json.Unmarshal(res.Data, g); err != nil {
		return nil, e.W(err, ECode040Q03)
	}

	return g, nil
}

// refresh refreshes the grant using the passed client id/secret
// if it is about to expire or if force is true
func (g *Grant) refresh(c *Client, clientID, secret string,
	force bool) (refreshed bool, err error) {

	// If not forced or isn't about to expire, then do nothing
	if !force && !g.IsAboutToExpireExpire() {
		return false, nil
	}

	ri := &RequestItem{
		Service: "core",
		Action:  "oauth2.Grant.refreshToken",
		Params: []interface{}{
			clientID,
			secret,
			g.RefreshToken,
		},
	}

	res, err := c.sendSingleRequestItem(c.deployment.getManageCoreServiceURL(), ri, nil)
	if err != nil {
		return false, e.W(err, ECode040Q04)
	}

	var tmpGrant *Grant
	if res.Data != nil {
		tmpGrant = &Grant{}
		if err := json.Unmarshal(res.Data, tmpGrant); err != nil {
			return false, e.W(err, ECode040Q05)
		}
	}

	g.Token = tmpGrant.Token
	g.TokenExpiry = tmpGrant.TokenExpiry
	if tmpGrant.RefreshToken != "" {
		g.RefreshToken = tmpGrant.RefreshToken
		g.RefreshTokenExpiry = tmpGrant.RefreshTokenExpiry
	}
	g.Scope = tmpGrant.Scope

	return true, nil
}

// Refresh calls refresh internally and saves to the DB
func GrantRefresh(ctx context.Context, db *sql.Connection, c *Client, credentialID int, token string) (g *Grant, err error) {
	credential, err := sqlmodel.CredentialGetByID(ctx, c.deployment.DB, credentialID)
	if err != nil {
		return nil, e.W(err, ECode040Q06)
	}

	dg, err := sqlmodel.DeploymentGrantGetByToken(ctx, db, token)
	if err != nil {
		return nil, e.N(ECode040Q07, e.MsgUnauthorized)
	}

	g = SQLDeploymentGrantToGrant(dg)
	if _, err := g.refresh(c, credential.ClientID, credential.ClientSecret, true); err != nil {
		return nil, e.W(err, ECode040Q08)
	}

	// Update the database record
	if err := sqlmodel.DeploymentGrantUpdate(ctx, db, dg.ID,
		&sqlmodel.DeploymentGrantUpdateParam{
			Token:              &g.Token,
			TokenExpiry:        &g.TokenExpiry,
			RefreshToken:       &g.RefreshToken,
			RefreshTokenExpiry: &g.RefreshTokenExpiry,
		}); err != nil {
		return nil, e.W(err, ECode040Q09)
	}

	return g, nil
}

// CleanExpiredGrants removes all grants where the refresh token has expired and is no longer usable.
// Note, the token should also be unusable as refresh tokens are periodically rotated when fetching
// new tokens
func CleanExpiredGrants(ctx context.Context, db *sql.Connection, c *Client) (err error) {
	if err := sqlmodel.DeploymentGrantPurgeByExpiredRefreshToken(ctx, db, int(time.Now().Unix())); err != nil {
		return e.W(err, ECode040Q0A)
	}

	return nil
}

// GetGrant returns the grant associated with the token if it exists
func GetGrant(ctx context.Context, db *sql.Connection, token string) (dg *model.DeploymentGrant, err error) {
	dg, err = sqlmodel.DeploymentGrantGetByToken(ctx, db, token)
	if err != nil {
		if e.ContainsError(err, sqlmodel.ECode040G01) {
			return nil, e.N(ECode040Q0B, "does not exist")
		}
		return nil, e.W(err, ECode040Q0C)
	}

	return dg, nil
}
