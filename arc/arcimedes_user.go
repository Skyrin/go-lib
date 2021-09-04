package arc

import (
	"encoding/json"
	"fmt"

	arcerrors "github.com/Skyrin/go-lib/arc/errors"
	gle "github.com/Skyrin/go-lib/errors"
)

// ArcimedesUser
type ArcimedesUser struct {
	ID         int        `json:"id"`
	ArcUserID  int        `json:"-"`
	Username   string     `json:"username"`
	Email      string     `json:"email"`
	Password   string     `json:"-"`
	FirstName  string     `json:"-"`
	MiddleName string     `json:"-"`
	LastName   string     `json:"-"`
	Person     CorePerson `json:"person"`
}

// RegisterArcimedesUser attempts to create the arcimedes user in arc. If the
// user already exists in arc, will fetch the arc user id (by the passed
// username) and then make the call to update it. This should only be called
// when registering a new arcimedes user, as it will reset the password
func (c *Client) RegisterArcimedesUser(ui *ArcimedesUser, retry bool) (au *ArcimedesUser, err error) {

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
		return nil, gle.Wrap(err, "ArcimedesUpsertUser.1", "")
	}
	res, err := c.sendSingleRequestItem(
		c.deployment.getManageArcimedesServiceURL(),
		ri,
		ca)
	if err != nil {
		if res != nil && res.ErrorCode == "E01FAAE" && retry {
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
				return nil, gle.Wrap(err, "RegisterCartCustomer.2", "")
			}

			// Try to upsert with the id now
			ui.ArcUserID = au.ID
			au, err = c.RegisterArcimedesUser(ui, false)
			if err != nil {
				return nil, gle.Wrap(err, "RegisterCartCustomer.3", "")
			}
		} else {
			return nil, gle.Wrap(err, "RegisterCartCustomer.4", "")
		}
	} else {
		au = &ArcimedesUser{}
		if err := json.Unmarshal(res.Data, au); err != nil {
			return nil, gle.Wrap(err, "ArcimedesUpsertUser.3", "")
		}
	}

	return au, nil
}

// ArcimedesUserGetByUsername fetches the arcimedes user by username
func (c *Client) ArcimedesUserGetByUsername(username string) (au *ArcimedesUser, err error) {
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
		return nil, gle.Wrap(err, "ArcimedesUserGetByUsername.1", "")
	}
	res, err := c.sendSingleRequestItem(
		c.deployment.getManageArcimedesServiceURL(),
		ri,
		ca)
	if err != nil {
		return nil, gle.Wrap(err, "ArcimedesUserGetByUsername.2", "")
	}

	auList := []*ArcimedesUser{}
	if err := json.Unmarshal(res.Data, &auList); err != nil {
		return nil, gle.Wrap(err, "ArcimedesUserGetByUsername.3", "")
	}

	if len(auList) != 1 {
		return nil, fmt.Errorf(arcerrors.ErrArcimedesUserNotExists)
	}

	return auList[0], nil
}
