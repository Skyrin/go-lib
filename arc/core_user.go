package arc

import (
	"encoding/json"
	"fmt"

	"github.com/Skyrin/go-lib/e"
)

const (
	ECode040401 = e.Code0404 + "01"
	ECode040402 = e.Code0404 + "02"
	ECode040403 = e.Code0404 + "03"
	ECode040404 = e.Code0404 + "04"
	ECode040405 = e.Code0404 + "05"
	ECode040406 = e.Code0404 + "06"
	ECode040407 = e.Code0404 + "07"
	ECode040408 = e.Code0404 + "08"
)

// RegisterCoreUser attempts to create the core user in arc. If the
// user already exists in arc, will fetch the arc user id (by the passed
// username) and then make the call to update it. This should only be called
// when registering a new core user, as it will reset the password
func (c *Client) RegisterCoreUser(ui *ArcUser, retry bool) (au *ArcUser, err error) {

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
	rio.Value["typeCode"] = ui.Type

	ri := &RequestItem{
		Service: "core",
		Action:  "User.update",
		Params:  params,
		Options: rio,
	}

	ca, err := c.getClientAuth()
	if err != nil {
		return nil, e.W(err, ECode040401)
	}

	res, err := c.sendSingleRequestItem(
		c.deployment.getManageCoreServiceURL(),
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
			au, err = c.CoreUserGetByUsername(ui.Username)
			if err != nil {
				return nil, e.W(err, ECode040402)
			}

			// Try to upsert with the id now
			ui.ArcUserID = au.ID
			au, err = c.RegisterCoreUser(ui, false)
			if err != nil {
				return nil, e.W(err, ECode040403)
			}
		} else {
			return nil, e.W(err, ECode040404)
		}
	} else {
		au = &ArcUser{}
		if err := json.Unmarshal(res.Data, au); err != nil {
			return nil, e.W(err, ECode040405)
		}
	}

	return au, nil
}

// CoreUserGetByUsername fetches the core user by username
func (c *Client) CoreUserGetByUsername(username string) (au *ArcUser, err error) {
	var params []interface{}

	rio := RequestItemOption{}
	rio.Filter = map[string]interface{}{}
	rio.Filter["username"] = username

	ri := &RequestItem{
		Service: "core",
		Action:  "User.get",
		Params:  params,
		Options: rio,
	}

	ca, err := c.getClientAuth()
	if err != nil {
		return nil, e.W(err, ECode040406)
	}
	res, err := c.sendSingleRequestItem(
		c.deployment.getManageCoreServiceURL(),
		ri,
		ca)
	if err != nil {
		return nil, e.W(err, ECode040407)
	}

	auList := []*ArcUser{}
	if err := json.Unmarshal(res.Data, &auList); err != nil {
		return nil, e.W(err, ECode040408)
	}

	if len(auList) != 1 {
		return nil, fmt.Errorf(e.MsgCoreUserNotExists)
	}

	return auList[0], nil
}
