package notifications

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/Skyrin/go-lib/errors"
)

const (
	// ArcPublishURL URL to publish notifications to Arc
	ArcPublishURL = "https://api-arc01a001.arcgrid.com/services/"
)

// ArcNotification formats a notification object for arc notifications
type ArcNotification struct {
	Format   string                    `json:"format"`
	Version  int                       `json:"version"`
	ID       int                       `json:"id"`
	Requests []ArcNotificationRequests `json:"requests"`
}

// ArcNotificationRequests formats the requests object
type ArcNotificationRequests struct {
	Service string        `json:"service"`
	Action  string        `json:"action"`
	Params  []interface{} `json:"params"`
}

// ArcNotificationResponse represents the notification service response
type ArcNotificationResponse struct {
	ID        int             `json:"id"`
	Success   bool            `json:"success"`
	Responses json.RawMessage `json:"responses"`
}

// SendArcNotification publishes a notification to arc
func SendArcNotification(an ArcNotification) error {
	payload := new(bytes.Buffer)
	json.NewEncoder(payload).Encode(an)

	req, err := http.NewRequest("POST", ArcPublishURL, payload)
	if err != nil {
		return errors.Wrap(err, "SendArcNotification.1", "")
	}

	req.Header.Add("Content-Type", "application/json")
	req.Header.Add("Accept", "application/json")

	c := &http.Client{}
	res, err := c.Do(req)
	if err != nil {
		return errors.Wrap(err, "SendArcNotification.2", "")
	}
	defer res.Body.Close()

	body := &ArcNotificationResponse{}
	decoder := json.NewDecoder(res.Body)
	if err := decoder.Decode(body); err != nil {
		return errors.Wrap(err, "SendArcNotification.3", "")
	}

	if !body.Success {
		return errors.Wrap(fmt.Errorf("%+v", body), "SendArcNotification.4", "")
	}

	return nil
}

// CreateArcNotificationRequest creates the notification request in JSON format
func CreateArcNotificationRequest(eventCode, publishKey, errorJSON string, version, id int) (an ArcNotification) {
	var params []interface{}
	params = append(params, eventCode)
	params = append(params, publishKey)
	params = append(params, errorJSON)

	req := ArcNotificationRequests{
		Service: "core",
		Action:  "open.arcsignal.Event.pub",
		Params:  params,
	}

	var requests []ArcNotificationRequests
	requests = append(requests, req)

	an = ArcNotification{
		Format:   "json",
		Version:  version,
		ID:       id,
		Requests: requests,
	}

	return an
}
