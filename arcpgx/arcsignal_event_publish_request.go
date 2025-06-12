package arcpgx

// createArcsignalEventPublishRequest creates a request object
func createArcsignalEventPublishRequest(eventCode, publishKey string,
	err interface{}) (r *RequestItem) {

	var params []interface{}
	params = append(params, eventCode)
	params = append(params, publishKey)
	params = append(params, err)

	return &RequestItem{
		Service: "core",
		Action:  "open.arcsignal.Event.pub",
		Params:  params,
	}
}

// SendArcsignalEventPublish creates and sends an arcsignal event publish API call
func (c *Client) SendArcsignalEventPublish(eventCode, publishKey string,
	eventErr interface{}) (err error) {

	// If event code not specified, then just ignore
	if eventCode == "" {
		return nil
	}

	ri := createArcsignalEventPublishRequest(eventCode, publishKey, eventErr)

	if _, err := c.sendSingleRequestItem(c.deployment.getAPICoreServiceURL(),
		ri, nil); err != nil {

		return err
	}

	return nil
}
