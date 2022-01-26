package arc

import (
	"encoding/json"
	"fmt"

	"github.com/Skyrin/go-lib/arc/model"
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
func (c *Client) RegisterArcimedesUser(iu *model.CoreUser, password string,
	retry bool) (cu *model.CoreUser, err error) {

	var params []interface{}
	if iu.ID > 0 {
		params = append(params, iu.ID)
	} else {
		params = append(params, nil)
	}

	rio := RequestItemOption{}
	rio.Value = map[string]interface{}{}
	rio.Value["username"] = iu.Email
	rio.Value["email"] = iu.Email
	rio.Value["password"] = password
	rio.Value["person"] = map[string]map[string]string{
		"value": map[string]string{
			"firstName":  iu.Person.FirstName,
			"middleName": iu.Person.MiddleName,
			"lastName":   iu.Person.LastName,
		},
	}

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
			cu, err = c.ArcimedesUserGetByUsername(iu.Username)
			if err != nil {
				return nil, e.W(err, ECode040202)
			}

			// Try to upsert with the id now
			iu.ID = cu.ID
			cu, err = c.RegisterArcimedesUser(iu, password, false)
			if err != nil {
				return nil, e.W(err, ECode040203)
			}
		} else {
			return nil, e.W(err, ECode040204)
		}
	} else {
		cu = &model.CoreUser{}
		if err := json.Unmarshal(res.Data, cu); err != nil {
			return nil, e.W(err, ECode040205)
		}
	}

	return cu, nil
}

// ArcimedesUserGetByUsername fetches the arcimedes user by username
func (c *Client) ArcimedesUserGetByUsername(username string) (cu *model.CoreUser, err error) {
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

	cuList := []*model.CoreUser{}
	if err := json.Unmarshal(res.Data, &cuList); err != nil {
		return nil, e.W(err, ECode040208)
	}

	if len(cuList) != 1 {
		return nil, fmt.Errorf(e.MsgArcimedesUserNotExists)
	}

	return cuList[0], nil
}
