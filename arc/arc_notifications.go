// Package arc provides the necessary calls to publish notifications to the arc system
// Basic Usage sample:
//
// 	Wherever the error comes from, convert to json
// 	errJSON, _ := json.Marshal(err2.Error())
//
// 	Create a new client
//	client := arc.NewClient()
//
// 	You also have the ability to set the URL and path by using SetBaseURL and SetPath
// 	Create a request, replace with the appropriate values for eventCode and publishKey
//	req := arc.CreateRequest("eventCode",
//		"publishKey",
//		string(errJSON))
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
	// DefaultBaseURL default URL to publish notifications to Arc
	DefaultBaseURL = "https://api-arc01a001.arcgrid.com"
	// DefaultPath is the default path to the arc notification service
	DefaultPath = "/services/"
	// DefaultVersion is the default version number to use in the request
	DefaultVersion = 1
	// DefaultID is the default id number to use in the request
	DefaultID = 0
)

// Notification formats a notification object for arc notifications
type Notification struct {
	Format      string    `json:"format"`
	Version     int       `json:"version"`
	ID          int       `json:"id"`
	RequestList []Request `json:"requests"`
}

// Request formats the request object
type Request struct {
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

// Client handles the process of publishing an arc notification
type Client struct {
	BaseURL     string
	Path        string
	Version     int
	ID          int
	RequestList []Request
}

// DefaultClient is a default client
var DefaultClient = &Client{
	BaseURL: DefaultBaseURL,
	Path:    DefaultPath,
	Version: DefaultVersion,
	ID:      DefaultID,
}

// NewClient returns a new client to handle requests to the arc notification service
func NewClient() (c *Client) {
	return DefaultClient
}

// SetBaseURL sets the base URL to the notification service
func SetBaseURL(url string) {
	DefaultClient.SetBaseURL(url)
}

// SetBaseURL sets the base URL to the notification service
func (c *Client) SetBaseURL(url string) {
	if len(url) == 0 {
		c.BaseURL = DefaultBaseURL
	} else {
		c.BaseURL = url
	}
}

// SetPath sets the path to the notification service
func SetPath(path string) {
	DefaultClient.SetPath(path)
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
func SetVersion(version int) {
	DefaultClient.SetVersion(version)
}

// SetVersion sets the version for the request to the notification service
func (c *Client) SetVersion(version int) {
	c.Version = version
}

// SetID sets the id for the request to the notification service
func SetID(id int) {
	DefaultClient.SetID(id)
}

// SetID sets the id for the request to the notification service
func (c *Client) SetID(id int) {
	c.ID = id
}

// AddRequest adds a notification request to the list of requests to send to arc
func (c *Client) AddRequest(req Request) {
	DefaultClient.RequestList = append(DefaultClient.RequestList, req)
}

// CreateRequest creates a request object
func CreateRequest(eventCode, publishKey, errorJSON string) (r Request) {
	var params []interface{}
	params = append(params, eventCode)
	params = append(params, publishKey)
	params = append(params, errorJSON)

	r = Request{
		Service: "core",
		Action:  "open.arcsignal.Event.pub",
		Params:  params,
	}

	return r
}

// Send performs the actual publish requet to the arc notification service
func (c *Client) Send() error {
	an, err := c.createArcNotification()
	if err != nil {
		return err
	}

	if err := c.sendArcNotification(an); err != nil {
		return err
	}

	return nil
}

// sendArcNotification sends the http request to publish a notification to arc
func (c *Client) sendArcNotification(an Notification) error {
	payload := new(bytes.Buffer)
	json.NewEncoder(payload).Encode(an)

	req, err := http.NewRequest("POST", c.getServiceURL(), payload)
	if err != nil {
		return errors.Wrap(err, "sendArcNotification.1", "")
	}

	req.Header.Add("Content-Type", "application/json")
	req.Header.Add("Accept", "application/json")

	client := &http.Client{}
	res, err := client.Do(req)
	if err != nil {
		return errors.Wrap(err, "sendArcNotification.2", "")
	}
	defer res.Body.Close()

	body := &ResponseList{}
	decoder := json.NewDecoder(res.Body)
	if err := decoder.Decode(body); err != nil {
		return errors.Wrap(err, "sendArcNotification.3", "")
	}

	fmt.Printf("body: %+v\n", body)

	if err := body.responseErrors(); err != nil {
		return errors.Wrap(err, "sendArcNotification.4", "")
	}

	return nil
}

// createArcNotification creates the notification request in JSON format
func (c *Client) createArcNotification() (an Notification, err error) {
	if len(c.RequestList) == 0 {
		return an, errors.Wrap(fmt.Errorf("Request List is empty"),
			"createArcNotification.1", "There needs to be at least one request to be able to send a notification")
	}

	return Notification{
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

	return nil
}
