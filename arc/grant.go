package arc

import (
	"encoding/json"
	"fmt"
	"time"

	gle "github.com/Skyrin/go-lib/errors"
)

// RegisterInput
type Oauth2Grant struct {
	Token              string  `json:"accessToken"`
	TokenExpiry        int     `json:"tokenExpiry"`
	Scope              string  `json:"scope"`
	RefreshToken       string  `json:"refreshTOken"`
	RefreshTokenExpiry int     `json:"refreshTokenExpiry"`
	TokenType          string  `json:"tokenType"`
	Client             *Client `json:"-"`
}

// clientCredentialsGrant get grant via client credentials
func (c *Client) clientCredentialsGrant(id, secret string) (g *Oauth2Grant, err error) {
	ri := &RequestItem{
		Service: "core",
		Action:  "oauth2.Grant.clientCredentials",
		Params: []interface{}{
			id, secret, "all",
		},
	}

	res, err := c.sendSingleRequestItem(c.deployment.getManageCoreServiceURL(), ri, false)
	if err != nil {
		return nil, gle.Wrap(err, "getClientCredentialsGrant.1", "")
	}

	if !res.Success {
		return nil, gle.Wrap(fmt.Errorf("[%s]%s", res.ErrorCode,
			res.Message), "getClientCredentialsGrant.2", "")
	}

	if res.Data != nil {
		g = &Oauth2Grant{}
		if err := json.Unmarshal(res.Data, g); err != nil {
			return nil, gle.Wrap(err, "getClientCredentialsGrant.3", "")
		}
	}

	// Add client to grant for quick reference to client id/secret
	g.Client = c

	return g, nil
}

// IsExpired returns if this grant's token has expired
func (g *Oauth2Grant) IsAboutToExpireExpire() bool {
	return g.TokenExpiry < int(time.Now().Unix())-60
}

// Refresh refreshes the token (if expired)
func (g *Oauth2Grant) Refresh(c *Client, force bool) (refreshed bool, err error) {
	// If it is going to expire in the next minute, then refresh it
	if !force && g.TokenExpiry > int(time.Now().Unix())-60 {
		return false, nil
	}

	ri := &RequestItem{
		Service: "core",
		Action:  "oauth2.Grant.refreshToken",
		Params: []interface{}{
			c.deployment.Model.ClientID, 
			c.deployment.Model.ClientSecret, 
			g.RefreshToken,
		},
	}
	
	res, err := c.sendSingleRequestItem(c.deployment.getManageCoreServiceURL(), ri, false)
	if err != nil {
		return false, gle.Wrap(err, "Oauth2Grant.Refresh.1", "")
	}

	var tmpGrant *Oauth2Grant
	if res.Data != nil {
		tmpGrant = &Oauth2Grant{}
		if err := json.Unmarshal(res.Data, tmpGrant); err != nil {
			return false, gle.Wrap(err, "Oauth2Grant.Refresh.3", "")
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
