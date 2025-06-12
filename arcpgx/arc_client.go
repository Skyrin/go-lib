// Package arc provides the necessary calls to publish notifications to the arc system
// Basic Usage sample:
//
//	Create a new client and set the base url for the service
//	client := arc.NewClient("https://example.com")
//
//	You also have the ability to set the URL and path by using SetBaseURL and SetPath
//	Create a request, replace with the appropriate values for eventCode and publishKey
//	req := arc.CreateArcsignalEventPublishRequest("eventCode",
//		"publishKey",
//		err)
//
//	Add at least one request, can add several
//	client.AddRequest(req)
//
//	Send the request
//	if err := client.Send(); err != nil {
//		return err
//	}
package arcpgx

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/Skyrin/go-lib/arcpgx/sqlmodel"
	"github.com/Skyrin/go-lib/e"
	sql "github.com/Skyrin/go-lib/sqlpgx"
	"github.com/rs/zerolog/log"
)

const (
	// DefaultPath is the default path to the arc notification service
	DefaultPath = "/services/"
	// DefaultVersion is the default version number to use in the request
	DefaultVersion = 1
	// DefaultID is the default id number to use in the request
	DefaultID = 0
	// Path for core API requests
	corePath = "/services/"
	// Path for arcimedes API requests
	arcimedesPath = "/apps/arcimedes/services/"
	// Path for cart API requests
	cartPath = "/apps/cart/stores/%s/services/"

	ECode040I01 = e.Code040I + "01"
	ECode040I02 = e.Code040I + "02"
	ECode040I03 = e.Code040I + "03"
	ECode040I04 = e.Code040I + "04"
	ECode040I05 = e.Code040I + "05"
	ECode040I06 = e.Code040I + "06"
	ECode040I07 = e.Code040I + "07"
	ECode040I08 = e.Code040I + "08"
	ECode040I09 = e.Code040I + "09"
	ECode040I0A = e.Code040I + "0A"
	ECode040I0B = e.Code040I + "0B"
	ECode040I0C = e.Code040I + "0C"
	ECode040I0D = e.Code040I + "0D"
	ECode040I0E = e.Code040I + "0E"
	ECode040I0F = e.Code040I + "0F"
	ECode040I0G = e.Code040I + "0G"
	ECode040I0H = e.Code040I + "0H"
	ECode040I0I = e.Code040I + "0I"
	ECode040I0J = e.Code040I + "0J"
	// ECode040I0K = e.Code040I + "0K"
	ECode040I0L = e.Code040I + "0L"
	ECode040I0M = e.Code040I + "0M"
	ECode040I0N = e.Code040I + "0N"
	ECode040I0O = e.Code040I + "0O"
	ECode040I0P = e.Code040I + "0P"
)

// Client handles the posting/making arc requests to an arc API server
type Client struct {
	BaseURL     string
	Path        string
	Version     int
	ID          int
	Username    string
	Token       string
	RequestList []*RequestItem
	// Defines the deployment this client is configured to connect to
	deployment *Deployment
	grant      *Grant // Defines grant used for authentication
}

// NewClient returns a new client to handle requests to the arc notification service
func NewClient(url string) (c *Client) {
	return &Client{
		BaseURL: url,
		Path:    DefaultPath,
		Version: DefaultVersion,
		ID:      DefaultID,
	}
}

// Close the client
func (c *Client) Close() (err error) {
	if c.deployment != nil && c.deployment.DB != nil {
		c.deployment.DB.DB.Close()
	}

	return nil
}

// NewClientFromDeployment initializes a client from the arc_deployments table
func NewClientFromDeployment(ctx context.Context, cp *sql.ConnParam, deploymentCode string) (c *Client, err error) {
	db, err := sql.NewPostgresConn(ctx, cp)
	if err != nil {
		return nil, e.W(err, ECode040I01)
	}

	d, err := sqlmodel.DeploymentGetByCode(ctx, db, deploymentCode)
	if err != nil {
		return nil, e.W(err, ECode040I02)
	}

	deployment, err := NewDeployment(ctx, db, cp, deploymentCode)
	if err != nil {
		return nil, e.W(err, ECode040I03)
	}

	c = &Client{
		BaseURL:    d.ManageURL,
		Path:       DefaultPath,
		Version:    DefaultVersion,
		ID:         DefaultID,
		deployment: deployment,
	}

	return c, nil
}

// Connect attempts to connect to the client
func (c *Client) Connect(ctx context.Context) (err error) {
	if c.deployment == nil {
		return e.N(ECode040I04, "no deployment configured")
	}

	// Initialize or refresh the authentication token
	if err = c.refresh(ctx, false); err != nil {
		return e.W(err, ECode040I05)
	}

	return nil
}

// SetBaseURL deprecated - sets the base URL to the notification service
func (c *Client) SetBaseURL(url string) {
	c.BaseURL = url
}

// SetPath deprecated - sets the path to the notification service
func (c *Client) SetPath(path string) {
	if len(path) == 0 {
		c.Path = DefaultPath
	} else {
		c.Path = path
	}
}

// SetVersion sets the version for the request to the notification service
func (c *Client) SetVersion(version int) {
	c.Version = version
}

// SetID sets the id for the request to the notification service
func (c *Client) SetID(id int) {
	c.ID = id
}

// SetUsername sets the usename to use
func (c *Client) SetUsername(username string) {
	c.Username = username
}

// SetToken sets the authentication token to use
func (c *Client) SetToken(token string) {
	c.Token = token
}

// AddRequest adds a request to the list of requests to send to arc
func (c *Client) AddRequest(req RequestItem) {
	c.RequestList = append(c.RequestList, &req)

	// TODO: if request size gets too large (too many requests), then
	// automatically send the current queue
}

// Flush sends whatever is in the current queue
func (c *Client) Flush() (resList *ResponseList, err error) {
	// TODO: implement
	return nil, e.N(ECode040I09, "not implemented yet")
}

// Send performs the actual publish requet to the arc notification service
func (c *Client) Send(ctx context.Context, reqItemList []*RequestItem) (resList *ResponseList, err error) {
	if len(c.RequestList) == 0 {
		return nil, e.N(ECode040I0A, "request list is empty")
	}

	ca, err := c.getClientAuth(ctx)
	if err != nil {
		return nil, e.W(err, ECode040I0B)
	}
	reqList := c.newRequestList(reqItemList)
	reqList.setAuth(ca)

	var url string
	if c.deployment != nil {
		if c.deployment.StoreCode != "" {
			url = c.deployment.getAPICartServiceURL(c.deployment.StoreCode)
		} else {
			url = c.deployment.getManageCoreServiceURL()
		}
	} else {
		url = c.getServiceURL()
	}

	resList, err = c.send(url, reqList, true)
	if err != nil {
		return resList, e.W(err, ECode040I0C)
	}

	c.RequestList = nil

	return resList, nil
}

func (c *Client) sendSingleRequestItem(url string, ri *RequestItem,
	ca *clientAuth) (res *Response, err error) {

	reqList := c.newRequestList([]*RequestItem{
		ri,
	})
	if ca != nil {
		reqList.setAuth(ca)
	}

	// If using an access token for authentication, then retry on failure
	resList, err := c.send(url, reqList, reqList.AccessToken != "")
	if err != nil {
		return nil, e.W(err, ECode040I0D)
	}

	if !resList.Responses[0].Success {
		return &resList.Responses[0],
			e.N(ECode040I0E,
				fmt.Sprintf("[%s]%s", resList.Responses[0].ErrorCode,
					resList.Responses[0].Message))
	}

	return &resList.Responses[0], nil
}

// send sends the http request to publish a notification to arc
func (c *Client) send(url string, r *RequestList,
	retryIfAuthFailure bool) (resList *ResponseList, err error) {

	payload := new(bytes.Buffer)
	json.NewEncoder(payload).Encode(r)

	req, err := http.NewRequest("POST", url, payload)
	if err != nil {
		return nil, e.W(err, ECode040I0F)
	}

	req.Header.Add("Content-Type", "application/json")
	req.Header.Add("Accept", "application/json")

	client := &http.Client{}
	res, err := client.Do(req)
	if err != nil {
		return nil, e.W(err, ECode040I0G)
	}
	defer res.Body.Close()

	resList = &ResponseList{}
	decoder := json.NewDecoder(res.Body)
	if err := decoder.Decode(resList); err != nil {
		return nil, e.W(err, ECode040I0H,
			fmt.Sprintf("url: %+v", req.URL))
	}

	if err := resList.responseErrors(); err != nil {
		return nil, e.W(err, ECode040I0I)
	}

	return resList, nil
}

// getServiceURL deprecated - returns the full url to post the request to
func (c *Client) getServiceURL() string {
	return fmt.Sprintf("%s%s", c.BaseURL, c.Path)
}

// GetDeployment return the currently set deployment
func (c *Client) GetDeployment() (d *Deployment) {
	return c.deployment
}

// getClientAuth gets the authentication associated with this client
func (c *Client) getClientAuth(ctx context.Context) (ca *clientAuth, err error) {
	if c == nil {
		return nil, nil
	}

	ca = &clientAuth{}
	if c.deployment != nil {
		if err := c.Connect(ctx); err != nil {
			return nil, e.W(err, ECode040I0J)
		}
		// reqList.AccessToken = c.grant.Token
		ca.accessToken = c.grant.Token
		return ca, nil
	} else if c.Token != "" {
		ca.token = c.Token
		if c.Username != "" {
			ca.username = c.Username
		}
	}

	// No auth configured, so return nil
	return nil, nil
}

// Log sends the extended error to the configured arc log. If not configured does nothing
func (c *Client) Log(ee *e.ExtendedError) {
	d := c.GetDeployment()
	if d == nil {
		// Deployment not configured, do nothing
		return
	}

	// formatting error message
	msg := fmt.Sprintf("%s\n%s", ee.Message, ee.Error())
	if err := c.SendArcsignalEventPublish(
		d.Model.LogEventCode, d.Model.LogPublishKey, msg); err != nil {
		log.Error().Err(err).Msgf("Error sending to arc log: %s\n%+v",
			ee.Message, ee.Error())
	}
}

// SetStoreCode sets the deployment's store code
func (c *Client) SetStoreCode(storeCode string) {
	if c.deployment != nil {
		c.deployment.StoreCode = storeCode
	}
}

// refresh refreshes the grant if it doesn't exist, is about to expire, or if
// force is true
func (c *Client) refresh(ctx context.Context, force bool) (err error) {

	if c.grant == nil {
		// Initialize grant
		c.grant = &Grant{
			Token:              c.deployment.Model.Token,
			TokenExpiry:        c.deployment.Model.TokenExpiry,
			RefreshToken:       c.deployment.Model.RefreshToken,
			RefreshTokenExpiry: c.deployment.Model.RefreshTokenExpiry,
		}
	}

	// If not forced or isn't about to expire, then do nothing
	if !force && !c.grant.IsAboutToExpireExpire() {
		return nil
	}

	// If no token yet or if the refresh token is about to, or has already expired, then
	// get using client credentials
	if c.grant.Token == "" || c.grant.RefreshTokenIsAboutToExpireExpire() {
		// If no access token then retrieve one from arc and save it
		c.grant, err = grantClientCredentials(c, c.deployment.Model.ClientID,
			c.deployment.Model.ClientSecret)
		if err != nil {
			return e.W(err, ECode040I0L)
		}

		// Update DB record
		if err := c.deployment.UpdateGrant(ctx, c.grant); err != nil {
			return e.W(err, ECode040I0M)
		}

		return nil
	}

	ri := &RequestItem{
		Service: "core",
		Action:  "oauth2.Grant.refreshToken",
		Params: []interface{}{
			c.deployment.Model.ClientID,
			c.deployment.Model.ClientSecret,
			c.grant.RefreshToken,
		},
	}

	res, err := c.sendSingleRequestItem(c.deployment.getManageCoreServiceURL(), ri, nil)
	if err != nil {
		return e.W(err, ECode040I0N)
	}

	var tmpGrant *Grant
	if res.Data != nil {
		tmpGrant = &Grant{}
		if err := json.Unmarshal(res.Data, tmpGrant); err != nil {
			return e.W(err, ECode040I0O)
		}
	}

	c.grant.Token = tmpGrant.Token
	c.grant.TokenExpiry = tmpGrant.TokenExpiry
	if tmpGrant.RefreshToken != "" {
		c.grant.RefreshToken = tmpGrant.RefreshToken
		c.grant.RefreshTokenExpiry = tmpGrant.RefreshTokenExpiry
	}
	c.grant.Scope = tmpGrant.Scope

	// Save new grant to DB
	if err := c.deployment.UpdateGrant(ctx, c.grant); err != nil {
		return e.W(err, ECode040I0P)
	}

	return nil
}
