package arc

import (
	"encoding/json"
	"fmt"

	"github.com/Skyrin/go-lib/e"
)

const (
	ECode040201 = e.Code0402 + "01"
	ECode040202 = e.Code0402 + "02"
	ECode040203 = e.Code0402 + "03"
	ECode040204 = e.Code0402 + "04"
	ECode040205 = e.Code0402 + "05"
	ECode040206 = e.Code0402 + "06"
	ECode040207 = e.Code0402 + "07"
	ECode040208 = e.Code0402 + "08"
)

// RegisterArcimedesUser attempts to create the arcimedes user in arc. If the
// user already exists in arc, will fetch the arc user id (by the passed
// username) and then make the call to update it. This should only be called
// when registering a new arcimedes user, as it will reset the password
func (c *Client) RegisterArcimedesUser(ui *ArcUser, retry bool) (au *ArcUser, err error) {

	var params []interface{}
	if ui.ArcUserID > 0 {
		params = append(params, ui.ArcUserID)
	} else {
		params = append(params, nil)
	}

	rio := RequestItemOption{}
	rio.Value = map[string]interface{}{}
	rio.Value["username"] = ui.Email
	rio.Value["email"] = ui.Email
	rio.Value["password"] = ui.Password
	rio.Value["firstName"] = ui.FirstName
	rio.Value["middleName"] = ui.MiddleName
	rio.Value["lastName"] = ui.LastName

	ri := &RequestItem{
		Service: "arcimedes",
		Action:  "User.update",
		Params:  params,
		Options: rio,
	}

	ca, err := c.getClientAuth()
	if err != nil {
		return nil, e.W(err, ECode040201)
	}
	res, err := c.sendSingleRequestItem(
		c.deployment.getManageArcimedesServiceURL(),
		ri,
		ca)
	if err != nil {
		if res != nil && res.ErrorCode == E01FAAE_UserAlreadyExists && retry {
			// User already exists in the system, The app is still requesting
			// to register the customer though, so maybe it did not save
			// properly. This may have happened if the call to arc succeeded
			// but something happened in the app before the response was
			// saved.
			// Since this app has permissions to create/update users, we will
			// assume the user needs to be recreated and will just update the
			// existing users information.
			// First now fetch that user
			au, err = c.ArcimedesUserGetByUsername(ui.Username)
			if err != nil {
				return nil, e.W(err, ECode040202)
			}

			// Try to upsert with the id now
			ui.ArcUserID = au.ID
			au, err = c.RegisterArcimedesUser(ui, false)
			if err != nil {
				return nil, e.W(err, ECode040203)
			}
		} else {
			return nil, e.W(err, ECode040204)
		}
	} else {
		au = &ArcUser{}
		if err := json.Unmarshal(res.Data, au); err != nil {
			return nil, e.W(err, ECode040205)
		}
	}

	return au, nil
}

// ArcimedesUserGetByUsername fetches the arcimedes user by username
func (c *Client) ArcimedesUserGetByUsername(username string) (au *ArcUser, err error) {
	var params []interface{}

	rio := RequestItemOption{}
	rio.Filter = map[string]interface{}{}
	rio.Filter["username"] = username

	ri := &RequestItem{
		Service: "arcimedes",
		Action:  "User.get",
		Params:  params,
		Options: rio,
	}

	ca, err := c.getClientAuth()
	if err != nil {
		return nil, e.W(err, ECode040206)
	}
	res, err := c.sendSingleRequestItem(
		c.deployment.getManageArcimedesServiceURL(),
		ri,
		ca)
	if err != nil {
		return nil, e.W(err, ECode040207)
	}

	auList := []*ArcUser{}
	if err := json.Unmarshal(res.Data, &auList); err != nil {
		return nil, e.W(err, ECode040208)
	}

	if len(auList) != 1 {
		return nil, fmt.Errorf(e.MsgArcimedesUserNotExists)
	}

	return auList[0], nil
}
