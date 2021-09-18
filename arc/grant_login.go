package arc

import (
	"encoding/json"
	"fmt"

	"github.com/Skyrin/go-lib/arc/sqlmodel"
	"github.com/Skyrin/go-lib/e"
)

// GrantUserinfo
type GrantUserinfo struct {
	ID         int    `json:"id"`
	Username   string `json:"username"`
	Email      string `json:"email"`
	FirstName  string `json:"firstName"`
	MiddleName string `json:"middleName"`
	LastName   string `json:"lastName"`
	TypeCode   string `json:"typeCode"`
	StatusCode string `json:"statusCode"`
	Created    int    `json:"created"`
}

// GrantLogin makes a call to login on behalf of an arc user. If successful, it will return a
// grant, with an access token and, if configured to use, a refresh token. The clientId used
// in this call must be from the configured arc deployment and will be tied to one of the arc
// apps (core, cart or arcimedes). Thus, if successful, the user that the grant is for will
// also be tied to that app (i.e. a core user, a cart customer or an arcimedes user).
// The grant will also be stored in the arc_deployment_grant table, along with a unique session
// so that a user in the same session can re-use the grant and the system can refresh the access
// token as needed (instead of generating new ones every time).
func (c *Client) GrantLogin(credentialID int, username, password string) (g *Grant, err error) {
	credential, err := sqlmodel.CredentialGetByID(c.deployment.DB, credentialID)
	if err != nil {
		return nil, e.Wrap(err, e.Code0401, "01")
	}

	params := []interface{}{
		credential.ClientID,
		username,
		password,
	}

	ri := &RequestItem{
		Service: "core",
		Action:  "oauth2.Grant.login",
		Params:  params,
	}

	ca, err := c.getClientAuth()
	if err != nil {
		return nil, e.Wrap(err, e.Code0401, "02")
	}
	res, err := c.sendSingleRequestItem(
		c.deployment.getManageCoreServiceURL(),
		ri,
		ca)
	if err != nil {
		return nil, e.Wrap(err, e.Code0401, "03")
	}

	g = &Grant{}
	if err := json.Unmarshal(res.Data, g); err != nil {
		return nil, e.Wrap(err, e.Code0401, "04")
	}

	// Get the arc user id associated with this token
	gui, err := c.GrantUserinfo(g.Token)
	if err != nil {
		return nil, e.Wrap(err, e.Code0401, "05")
	}

	// Save the grant in the arc_deployment_grant table.
	// Initial implementation will treat the token like a session. However, it will
	// use the refresh token's expiry to determine when the token has expired.
	// The token as exposed to apps calling this api will be:
	// { accessToken, expiry, refreshExpiry }
	// This app must implement a 'refreshToken' API (the API name can be whatever
	// the app deems best). The logic should call the GrantRefresh method, which
	// will try to refresh the token, if it has not expired. If it has expired it
	// will return an error.
	//
	// If in the future a real session is needed, this table/logic will be modifed
	if _, err := sqlmodel.DeploymentGrantInsert(c.deployment.DB, &sqlmodel.DeploymentGrantInsertParam{
		DeploymentID:       c.deployment.Model.ID,
		ArcUserID:          gui.ID,
		CredentialID:       credential.ID,
		Token:              g.Token,
		TokenExpiry:        g.TokenExpiry,
		RefreshToken:       g.RefreshToken,
		RefreshTokenExpiry: g.RefreshTokenExpiry,
	}); err != nil {
		return nil, e.Wrap(err, e.Code0401, "06")
	}

	return g, nil
}

// GrantUserinfo makes a call oauth2.Grant.userinfo with the specified access token
// to fetch the user info associated with the oauth access token
func (c *Client) GrantUserinfo(accessToken string) (gui *GrantUserinfo, err error) {
	// Double check user exists in our system

	params := []interface{}{
		accessToken,
	}

	ri := &RequestItem{
		Service: "core",
		Action:  "oauth2.Grant.userinfo",
		Params:  params,
	}

	res, err := c.sendSingleRequestItem(
		c.deployment.getManageCoreServiceURL(),
		ri,
		&clientAuth{
			accessToken: accessToken,
		})
	if err != nil {
		return nil, e.Wrap(err, e.Code0402, "01")
	}

	gui = &GrantUserinfo{}
	if err := json.Unmarshal(res.Data, gui); err != nil {
		return nil, e.Wrap(err, e.Code0402, "02")
	}

	return gui, nil
}

// GrantRevoke removes the access token from the arc_deployment_grant table and makes a call
// to the associated arc deployment to revoke the grant (access token)
func (c *Client) GrantRevoke(accessToken string) (err error) {
	if accessToken == "" {
		return nil
	}

	// Purge the grant from the table
	if err := sqlmodel.DeploymentGrantPurgeByToken(c.deployment.DB, accessToken); err != nil {
		return e.Wrap(err, e.Code0403, "01")
	}

	// Now revoke the grant in arc
	params := []interface{}{}

	ri := &RequestItem{
		Service: "core",
		Action:  "oauth2.Grant.revoke",
		Params:  params,
	}

	ca := &clientAuth{accessToken: accessToken}
	_, err = c.sendSingleRequestItem(
		c.deployment.getManageCoreServiceURL(),
		ri,
		ca)
	if err != nil {
		// If got authorization failed due to not being logged in, then just return nil
		if e.ContainsError(err, E01F1A8_AuthorizationFailed) {
			return nil
		}
		return e.Wrap(err, e.Code0403, "03",
			fmt.Sprintf("url: %s", c.deployment.getManageCoreServiceURL()))
	}

	return nil
}
