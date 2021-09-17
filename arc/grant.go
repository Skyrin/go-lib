package arc

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/Skyrin/go-lib/arc/model"
	"github.com/Skyrin/go-lib/arc/sqlmodel"
	"github.com/Skyrin/go-lib/e"
	"github.com/Skyrin/go-lib/sql"
)

// Grant
type Grant struct {
	Token              string `json:"accessToken"`
	TokenExpiry        int    `json:"tokenExpiry"`
	Scope              string `json:"scope"`
	RefreshToken       string `json:"refreshToken"`
	RefreshTokenExpiry int    `json:"refreshTokenExpiry"`
	TokenType          string `json:"tokenType"`
}

// IsExpired returns if this grant's token has expired
func (g *Grant) IsAboutToExpireExpire() bool {
	return g.TokenExpiry < int(time.Now().Unix())-60
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
		return nil, e.Wrap(err, e.Code040N, "01")
	}

	if !res.Success {

		return nil, e.Wrap(err, e.Code040N, "02",
			fmt.Sprintf("[%s]%s", res.ErrorCode, res.Message))
	}

	g = &Grant{}
	if err := json.Unmarshal(res.Data, g); err != nil {
		return nil, e.Wrap(err, e.Code040N, "03")
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
		return false, e.Wrap(err, e.Code040O, "01")
	}

	var tmpGrant *Grant
	if res.Data != nil {
		tmpGrant = &Grant{}
		if err := json.Unmarshal(res.Data, tmpGrant); err != nil {
			return false, e.Wrap(err, e.Code040O, "02")
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
func GrantRefresh(db *sql.Connection, c *Client, credentialID int, token string) (g *Grant, err error) {
	credential, err := sqlmodel.CredentialGetByID(c.deployment.DB, credentialID)
	if err != nil {
		return nil, e.Wrap(err, e.Code040P, "01")
	}

	dg, err := sqlmodel.DeploymentGrantGetByToken(db, token)
	if err != nil {
		return nil, e.New(e.Code040P, "01", e.MsgUnauthorized)
	}

	g = SQLDeploymentGrantToGrant(dg)
	if _, err := g.refresh(c, credential.ClientID, credential.ClientSecret, true); err != nil {
		return nil, e.Wrap(err, e.Code040P, "02")
	}

	// Update the database record
	if err := sqlmodel.DeploymentGrantUpdate(db, dg.ID,
		&sqlmodel.DeploymentGrantUpdateParam{
			Token:              &g.Token,
			TokenExpiry:        &g.TokenExpiry,
			RefreshToken:       &g.RefreshToken,
			RefreshTokenExpiry: &g.RefreshTokenExpiry,
		}); err != nil {
		return nil, e.Wrap(err, e.Code040P, "03")
	}

	return g, nil
}
