package arcpgx

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/Skyrin/go-lib/arcpgx/model"
	"github.com/Skyrin/go-lib/e"
)

const (
	ECode040K01 = e.Code040K + "01"
	ECode040K02 = e.Code040K + "02"
	ECode040K03 = e.Code040K + "03"
	ECode040K04 = e.Code040K + "04"
	ECode040K05 = e.Code040K + "05"
	ECode040K06 = e.Code040K + "06"
	ECode040K07 = e.Code040K + "07"
	ECode040K08 = e.Code040K + "08"
)

// RegisterCartCustomer attempts to create the cart customer in arc. If the
// customer already exists in arc, will fetch the arc user id (by the passed
// username) and then make the call to update it. This should only be called
// when registering a new cart customer, as it will reset the password
func (c *Client) RegisterCartCustomer(ctx context.Context, storeCode string, iu *model.CoreUser,
	password string, retry bool) (cust *model.CoreUser, err error) {

	var params []interface{}
	if iu.ID > 0 {
		params = append(params, iu.ID)
	} else {
		params = append(params, nil)
	}

	rio := RequestItemOption{}
	rio.Value = map[string]interface{}{}
	rio.Value["username"] = iu.Username
	rio.Value["email"] = iu.Email
	if password != "" {
		rio.Value["password"] = password
	}
	rio.Value["person"] = map[string]map[string]string{
		"value": map[string]string{
			"firstName":  iu.Person.FirstName,
			"middleName": iu.Person.MiddleName,
			"lastName":   iu.Person.LastName,
		},
	}

	ri := &RequestItem{
		Service: "cart",
		Action:  "Customer.update",
		Params:  params,
		Options: rio,
	}

	ca, err := c.getClientAuth(ctx)
	if err != nil {
		return nil, e.W(err, ECode040K01)
	}
	res, err := c.sendSingleRequestItem(
		c.deployment.getManageCartServiceURL(storeCode),
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
			cust, err = c.CartGetCustomerByUsername(ctx, storeCode, iu.Username)
			if err != nil {
				return nil, e.W(err, ECode040K02)
			}

			// Try to upsert with the id now
			iu.ID = cust.ID
			cust, err = c.RegisterCartCustomer(ctx, storeCode, iu, password, false)
			if err != nil {
				return nil, e.W(err, ECode040K03)
			}
		} else {
			return nil, e.W(err, ECode040K04)
		}
	} else {
		cust = &model.CoreUser{}
		if err := json.Unmarshal(res.Data, cust); err != nil {
			return nil, e.W(err, ECode040K05)
		}
	}

	return cust, nil
}

// CartGetCustomerByUsername fetches the customer by username from the specified store
func (c *Client) CartGetCustomerByUsername(ctx context.Context, storeCode, username string) (
	cust *model.CoreUser, err error) {

	var params []interface{}

	rio := RequestItemOption{}
	rio.Filter = map[string]interface{}{}
	rio.Filter["username"] = username

	ri := &RequestItem{
		Service: "cart",
		Action:  "Customer.get",
		Params:  params,
		Options: rio,
	}

	ca, err := c.getClientAuth(ctx)
	if err != nil {
		return nil, e.W(err, ECode040K06)
	}
	res, err := c.sendSingleRequestItem(
		c.deployment.getManageCartServiceURL(storeCode),
		ri,
		ca)
	if err != nil {
		return nil, e.W(err, ECode040K07)
	}

	custList := []*model.CoreUser{}
	if err := json.Unmarshal(res.Data, &custList); err != nil {
		return nil, e.W(err, ECode040K08)
	}

	if len(custList) != 1 {
		return nil, fmt.Errorf(e.MsgCartCustomerNotExists)
	}

	return custList[0], nil
}
