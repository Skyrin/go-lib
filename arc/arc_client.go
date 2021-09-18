// Package arc provides the necessary calls to publish notifications to the arc system
// Basic Usage sample:
//
// 	Create a new client and set the base url for the service
//	client := arc.NewClient("https://example.com")
//
// 	You also have the ability to set the URL and path by using SetBaseURL and SetPath
// 	Create a request, replace with the appropriate values for eventCode and publishKey
//	req := arc.CreateArcsignalEventPublishRequest("eventCode",
//		"publishKey",
//		err)
//
// 	Add at least one request, can add several
//	client.AddRequest(req)
//
// 	Send the request
//	if err := client.Send(); err != nil {
//		return err
//	}
package arc

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/Skyrin/go-lib/arc/sqlmodel"
	"github.com/Skyrin/go-lib/e"
	"github.com/Skyrin/go-lib/sql"
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
func (c *Client) Close() {
	if c.deployment != nil && c.deployment.DB != nil {
		_ = c.deployment.DB.DB.Close()
	}
}

// NewClientFromDeployment initializes a client from the arc_deployments table
func NewClientFromDeployment(cp *sql.ConnParam,
	deploymentCode, storeCode string) (c *Client, err error) {

	db, err := sql.NewPostgresConn(cp)
	if err != nil {
		return nil, e.Wrap(err, e.Code0405, "01")
	}

	d, err := sqlmodel.DeploymentGetByCode(db, deploymentCode)
	if err != nil {
		return nil, e.Wrap(err, e.Code0405, "02")
	}

	deployment, err := NewDeployment(db, deploymentCode)
	if err != nil {
		return nil, e.Wrap(err, e.Code0405, "03")
	}

	deployment.StoreCode = storeCode

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
func (c *Client) Connect() (err error) {
	if c.deployment == nil {
		return e.New(e.Code0406, "01", "no deployment configured")
	}

	if c.deployment.Model.Token == "" {
		// If no access token then retrieve one from arc and save it
		g, err := grantClientCredentials(c, c.deployment.Model.ClientID,
			c.deployment.Model.ClientSecret)
		if err != nil {
			return e.Wrap(err, e.Code0406, "02")
		}
		c.grant = g
		// Update DB record
		if err := c.deployment.UpdateGrant(g); err != nil {
			return e.Wrap(err, e.Code0406, "03")
		}
		return nil
	}

	// Else, ensure the token is valid/refreshed
	c.grant = &Grant{
		Token:              c.deployment.Model.Token,
		TokenExpiry:        c.deployment.Model.TokenExpiry,
		RefreshToken:       c.deployment.Model.RefreshToken,
		RefreshTokenExpiry: c.deployment.Model.RefreshTokenExpiry,
	}

	// Ensure the token is valid
	refreshed, err := c.grant.refresh(c, c.deployment.Model.ClientID,
		c.deployment.Model.ClientSecret, false)
	if err != nil {
		if e.ContainsError(err, e.MsgInvalidGrant) {
			// Failed to refresh, maybe refresh token expired - so try
			// to get using client credentials
			c.deployment.Model.Token = ""
			return c.Connect()
		}
		return e.Wrap(err, e.Code0406, "04")
	}

	// If it was refreshed, then save to DB
	if refreshed {
		if err := c.deployment.UpdateGrant(c.grant); err != nil {
			return e.Wrap(err, e.Code0406, "05")
		}
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
	return nil, e.New(e.Code0407, "02", "not implemented yet")
}

// Send performs the actual publish requet to the arc notification service
func (c *Client) Send(reqItemList []*RequestItem) (resList *ResponseList, err error) {
	if len(c.RequestList) == 0 {
		return nil, e.New(e.Code0408, "01", "request list is empty")
	}

	ca, err := c.getClientAuth()
	if err != nil {
		return nil, e.Wrap(err, e.Code0408, "02")
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
		return resList, e.Wrap(err, e.Code0408, "03")
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
		return nil, e.Wrap(err, e.Code0409, "01")
	}

	if !resList.Responses[0].Success {
		return &resList.Responses[0],
			e.New(e.Code0409, "02",
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
		return nil, e.Wrap(err, e.Code040A, "01")
	}

	req.Header.Add("Content-Type", "application/json")
	req.Header.Add("Accept", "application/json")

	client := &http.Client{}
	res, err := client.Do(req)
	if err != nil {
		return nil, e.Wrap(err, e.Code040A, "02")
	}
	defer res.Body.Close()

	resList = &ResponseList{}
	decoder := json.NewDecoder(res.Body)
	if err := decoder.Decode(resList); err != nil {
		return nil, e.Wrap(err, e.Code040A, "01",
			fmt.Sprintf("url: %+v", req.URL))
	}

	if err := resList.responseErrors(); err != nil {
		return nil, e.Wrap(err, e.Code040A, "04")
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
func (c *Client) getClientAuth() (ca *clientAuth, err error) {
	if c == nil {
		return nil, nil
	}

	ca = &clientAuth{}
	if c.deployment != nil {
		if err := c.Connect(); err != nil {
			return nil, e.Wrap(err, e.Code040B, "01")
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
