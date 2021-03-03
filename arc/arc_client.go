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

	"github.com/Skyrin/go-lib/errors"
)

const (
	// DefaultPath is the default path to the arc notification service
	DefaultPath = "/services/"
	// DefaultVersion is the default version number to use in the request
	DefaultVersion = 1
	// DefaultID is the default id number to use in the request
	DefaultID = 0
)

// Request formats a request to send to an arc API server
type Request struct {
	Format      string        `json:"format"`
	Version     int           `json:"version"`
	ID          int           `json:"id"`
	RequestList []RequestItem `json:"requests"`
	Token       string        `json:"token"`
	Username    string        `json:"username"`
}

// RequestItem is an item from a RequestList
type RequestItem struct {
	Service string        `json:"service"`
	Action  string        `json:"action"`
	Params  []interface{} `json:"params"`
}

// ResponseList represents the notification service response
type ResponseList struct {
	ID        int        `json:"id"`
	Success   bool       `json:"success"`
	Responses []Response `json:"responses"`
}

// Response represents the response from Arc
type Response struct {
	ID        int             `json:"id"`
	Success   bool            `json:"success"`
	Code      int             `json:"code"`
	ErrorCode string          `json:"errorCode"`
	Message   string          `json:"message"`
	Data      json.RawMessage `json:"data"`
	Errors    []string        `json:"errors"`
}

// Client handles the posting/making arc requests to an arc API server
type Client struct {
	BaseURL     string
	Path        string
	Version     int
	ID          int
	Username    string
	Token       string
	RequestList []RequestItem
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

// SetBaseURL sets the base URL to the notification service
func (c *Client) SetBaseURL(url string) {
	c.BaseURL = url
}

// SetPath sets the path to the notification service
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
	c.RequestList = append(c.RequestList, req)
}

// CreateArcsignalEventPublishRequest creates a request object
func CreateArcsignalEventPublishRequest(eventCode, publishKey string, err interface{}) (r RequestItem) {
	var params []interface{}
	params = append(params, eventCode)
	params = append(params, publishKey)
	params = append(params, err)

	r = RequestItem{
		Service: "core",
		Action:  "open.arcsignal.Event.pub",
		Params:  params,
	}

	return r
}

// Send performs the actual publish requet to the arc notification service
func (c *Client) Send() error {
	an, err := c.createArcRequest()
	if err != nil {
		return err
	}

	if err := c.sendArcRequest(an); err != nil {
		return err
	}

	c.RequestList = nil

	return nil
}

// sendArcRequest sends the http request to publish a notification to arc
func (c *Client) sendArcRequest(ar Request) error {
	payload := new(bytes.Buffer)
	json.NewEncoder(payload).Encode(ar)

	req, err := http.NewRequest("POST", c.getServiceURL(), payload)
	if err != nil {
		return errors.Wrap(err, "sendArcRequest.1", "")
	}

	req.Header.Add("Content-Type", "application/json")
	req.Header.Add("Accept", "application/json")

	client := &http.Client{}
	res, err := client.Do(req)
	if err != nil {
		return errors.Wrap(err, "sendArcRequest.2", "")
	}
	defer res.Body.Close()

	body := &ResponseList{}
	decoder := json.NewDecoder(res.Body)
	if err := decoder.Decode(body); err != nil {
		return errors.Wrap(err, "sendArcRequest.3", "")
	}

	if err := body.responseErrors(); err != nil {
		return errors.Wrap(err, "sendArcRequest.4", "")
	}

	return nil
}

// createArcRequest creates the notification request in JSON format
func (c *Client) createArcRequest() (ar Request, err error) {
	if len(c.RequestList) == 0 {
		return ar, errors.Wrap(fmt.Errorf("Request List is empty"),
			"createArcRequest.1", "There needs to be at least one request to be able to send a notification")
	}

	return Request{
		Format:      "json",
		Version:     c.Version,
		ID:          c.ID,
		RequestList: c.RequestList,
	}, nil
}

// getServiceURL returns the full url to post the request to
func (c *Client) getServiceURL() string {
	return fmt.Sprintf("%s%s", c.BaseURL, c.Path)
}

// responseErrors returns errors found in the response if any.  Can add other checks for errors
func (nrl *ResponseList) responseErrors() error {
	if !nrl.Success {
		return errors.Wrap(fmt.Errorf("%+v", nrl), "responseErrors.1", "")
	}

	for _, v := range nrl.Responses {
		if !v.Success {
			return errors.Wrap(fmt.Errorf("%+v", nrl), "responseErrors.2", "")
		}
	}

	return nil
}
