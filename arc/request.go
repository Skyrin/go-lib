package arc

import (
	"github.com/Skyrin/go-lib/errors"
)

// RequestList formats a request to send to an arc API server
type RequestList struct {
	Format      string         `json:"format"`
	Version     int            `json:"version"`
	ID          int            `json:"id"`
	Requests    []*RequestItem `json:"requests"`
	Token       string         `json:"token"`
	Username    string         `json:"username"`
	AccessToken string         `json:"accessToken"`
}

// RequestItem is an item from a RequestList
type RequestItem struct {
	Service string            `json:"service"`
	Action  string            `json:"action"`
	Params  []interface{}     `json:"params"`
	Options RequestItemOption `json:"options"`
}

// RequestItemOption defines possible options for a request item
type RequestItemOption struct {
	Value  map[string]interface{} `json:"value"`
	Flag   map[string]bool        `json:"flag"`
	Filter map[string]interface{} `json:"filter"`
}

// newRequestList creates the notification request in JSON format
func (c *Client) newRequestList(reqItemList []*RequestItem) (reqList *RequestList) {
	version := DefaultVersion
	id := DefaultID

	if c != nil {
		version = c.Version
		id = c.ID
	}
	reqList = &RequestList{
		Format:   "json",
		Version:  version,
		ID:       id,
		Requests: reqItemList,
	}

	return reqList
}

// setRequestListAuth sets the authentication parameters to be used in the request call
func (reqList *RequestList) setAuth(c *Client) (err error) {
	if c == nil {
		return
	}

	if c.deployment != nil {
		if err := c.Connect(); err != nil {
			return errors.Wrap(err, "RequestList.setAuth.1", "")
		}
		reqList.AccessToken = c.grant.Token
	} else {
		if c.Username != "" {
			reqList.Username = c.Username
		}
		if c.Token != "" {
			reqList.Token = c.Token
		}
	}

	return nil
}
