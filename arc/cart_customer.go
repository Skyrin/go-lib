package arc

import (
	"encoding/json"
	"fmt"

	"github.com/Skyrin/go-lib/e"
)

// RegisterCartCustomer attempts to create the cart customer in arc. If the
// customer already exists in arc, will fetch the arc user id (by the passed
// username) and then make the call to update it. This should only be called
// when registering a new cart customer, as it will reset the password
func (c *Client) RegisterCartCustomer(storeCode string,
	ci *ArcUser, retry bool) (cust *ArcUser, err error) {

	var params []interface{}
	if ci.ArcUserID > 0 {
		params = append(params, ci.ArcUserID)
	} else {
		params = append(params, nil)
	}

	rio := RequestItemOption{}
	rio.Value = map[string]interface{}{}
	rio.Value["username"] = ci.Username
	rio.Value["email"] = ci.Email
	rio.Value["password"] = ci.Password
	rio.Value["firstName"] = ci.FirstName
	rio.Value["middleName"] = ci.MiddleName
	rio.Value["lastName"] = ci.LastName

	ri := &RequestItem{
		Service: "cart",
		Action:  "Customer.update",
		Params:  params,
		Options: rio,
	}

	ca, err := c.getClientAuth()
	if err != nil {
		return nil, e.Wrap(err, e.Code040E, "01")
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
			cust, err = c.CartGetCustomerByUsername(storeCode, ci.Username)
			if err != nil {
				return nil, e.Wrap(err, e.Code040E, "02")
			}

			// Try to upsert with the id now
			ci.ArcUserID = cust.ID
			cust, err = c.RegisterCartCustomer(storeCode, ci, false)
			if err != nil {
				return nil, e.Wrap(err, e.Code040E, "02")
			}
		} else {
			return nil, e.Wrap(err, e.Code040E, "04")
		}
	} else {
		cust = &ArcUser{}
		if err := json.Unmarshal(res.Data, cust); err != nil {
			return nil, e.Wrap(err, e.Code040E, "05")
		}
	}

	return cust, nil
}

// CartGetCustomerByUsername fetches the customer by username from the specified store
func (c *Client) CartGetCustomerByUsername(storeCode, username string) (cust *ArcUser, err error) {
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

	ca, err := c.getClientAuth()
	if err != nil {
		return nil, e.Wrap(err, e.Code040F, "01")
	}
	res, err := c.sendSingleRequestItem(
		c.deployment.getManageCartServiceURL(storeCode),
		ri,
		ca)
	if err != nil {
		return nil, e.Wrap(err, e.Code040F, "02")
	}

	custList := []*ArcUser{}
	if err := json.Unmarshal(res.Data, &custList); err != nil {
		return nil, e.Wrap(err, e.Code040F, "03")
	}

	if len(custList) != 1 {
		return nil, fmt.Errorf(e.MsgCartCustomerNotExists)
	}

	return custList[0], nil
}
