package arcpgx

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/Skyrin/go-lib/arcpgx/model"
	"github.com/Skyrin/go-lib/e"
)

const (
	ECode040L01 = e.Code040L + "01"
	ECode040L02 = e.Code040L + "02"
	ECode040L03 = e.Code040L + "03"
	ECode040L04 = e.Code040L + "04"
	ECode040L05 = e.Code040L + "05"
	ECode040L06 = e.Code040L + "06"
	ECode040L07 = e.Code040L + "07"
	ECode040L08 = e.Code040L + "08"
)

// RegisterCoreUser attempts to create the core user in arc. If the
// user already exists in arc, will fetch the arc user id (by the passed
// username) and then make the call to update it. This should only be called
// when registering a new core user, as it will reset the password
func (c *Client) RegisterCoreUser(ctx context.Context, iu *model.CoreUser, password string,
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
	rio.Value["typeCode"] = iu.Type
	rio.Value["person"] = map[string]map[string]string{
		"value": map[string]string{
			"firstName":  iu.Person.FirstName,
			"middleName": iu.Person.MiddleName,
			"lastName":   iu.Person.LastName,
		},
	}

	ri := &RequestItem{
		Service: "core",
		Action:  "User.update",
		Params:  params,
		Options: rio,
	}

	ca, err := c.getClientAuth(ctx)
	if err != nil {
		return nil, e.W(err, ECode040L01)
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
			cu, err = c.CoreUserGetByUsername(ctx, iu.Username)
			if err != nil {
				return nil, e.W(err, ECode040L02)
			}

			// Try to upsert with the id now
			iu.ID = cu.ID
			cu, err = c.RegisterCoreUser(ctx, iu, password, false)
			if err != nil {
				return nil, e.W(err, ECode040L03)
			}
		} else {
			return nil, e.W(err, ECode040L04)
		}
	} else {
		cu = &model.CoreUser{}
		if err := json.Unmarshal(res.Data, cu); err != nil {
			return nil, e.W(err, ECode040L05)
		}
	}

	return cu, nil
}

// CoreUserGetByUsername fetches the core user by username
func (c *Client) CoreUserGetByUsername(ctx context.Context, username string) (cu *model.CoreUser, err error) {
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

	ca, err := c.getClientAuth(ctx)
	if err != nil {
		return nil, e.W(err, ECode040L06)
	}
	res, err := c.sendSingleRequestItem(
		c.deployment.getManageCoreServiceURL(),
		ri,
		ca)
	if err != nil {
		return nil, e.W(err, ECode040L07)
	}

	cuList := []*model.CoreUser{}
	if err := json.Unmarshal(res.Data, &cuList); err != nil {
		return nil, e.W(err, ECode040L08)
	}

	if len(cuList) != 1 {
		return nil, fmt.Errorf(e.MsgCoreUserNotExists)
	}

	return cuList[0], nil
}
