package arcpgx

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/Skyrin/go-lib/arcpgx/model"
	"github.com/Skyrin/go-lib/e"
)

const (
	ECode040J01 = e.Code040J + "01"
	ECode040J02 = e.Code040J + "02"
	ECode040J03 = e.Code040J + "03"
	ECode040J04 = e.Code040J + "04"
	ECode040J05 = e.Code040J + "05"
	ECode040J06 = e.Code040J + "06"
	ECode040J07 = e.Code040J + "07"
	ECode040J08 = e.Code040J + "08"
)

// RegisterArcimedesUser attempts to create the arcimedes user in arc. If the
// user already exists in arc, will fetch the arc user id (by the passed
// username) and then make the call to update it. This should only be called
// when registering a new arcimedes user, as it will reset the password
func (c *Client) RegisterArcimedesUser(ctx context.Context, iu *model.CoreUser, password string,
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

	ca, err := c.getClientAuth(ctx)
	if err != nil {
		return nil, e.W(err, ECode040J01)
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
			cu, err = c.ArcimedesUserGetByUsername(ctx, iu.Username)
			if err != nil {
				return nil, e.W(err, ECode040J02)
			}

			// Try to upsert with the id now
			iu.ID = cu.ID
			cu, err = c.RegisterArcimedesUser(ctx, iu, password, false)
			if err != nil {
				return nil, e.W(err, ECode040J03)
			}
		} else {
			return nil, e.W(err, ECode040J04)
		}
	} else {
		cu = &model.CoreUser{}
		if err := json.Unmarshal(res.Data, cu); err != nil {
			return nil, e.W(err, ECode040J05)
		}
	}

	return cu, nil
}

// ArcimedesUserGetByUsername fetches the arcimedes user by username
func (c *Client) ArcimedesUserGetByUsername(ctx context.Context, username string) (cu *model.CoreUser, err error) {
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

	ca, err := c.getClientAuth(ctx)
	if err != nil {
		return nil, e.W(err, ECode040J06)
	}
	res, err := c.sendSingleRequestItem(
		c.deployment.getManageArcimedesServiceURL(),
		ri,
		ca)
	if err != nil {
		return nil, e.W(err, ECode040J07)
	}

	cuList := []*model.CoreUser{}
	if err := json.Unmarshal(res.Data, &cuList); err != nil {
		return nil, e.W(err, ECode040J08)
	}

	if len(cuList) != 1 {
		return nil, fmt.Errorf(e.MsgArcimedesUserNotExists)
	}

	return cuList[0], nil
}
