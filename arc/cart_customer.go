package arc

import (
	"encoding/json"
	"fmt"

	"github.com/Skyrin/go-lib/arc/model"
	"github.com/Skyrin/go-lib/e"
)

const (
	ECode040301 = e.Code0403 + "01"
	ECode040302 = e.Code0403 + "02"
	ECode040303 = e.Code0403 + "03"
	ECode040304 = e.Code0403 + "04"
	ECode040305 = e.Code0403 + "05"
	ECode040306 = e.Code0403 + "06"
	ECode040307 = e.Code0403 + "07"
	ECode040308 = e.Code0403 + "08"
)

// RegisterCartCustomer attempts to create the cart customer in arc. If the
// customer already exists in arc, will fetch the arc user id (by the passed
// username) and then make the call to update it. This should only be called
// when registering a new cart customer, as it will reset the password
func (c *Client) RegisterCartCustomer(storeCode string, iu *model.CoreUser,
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

	ca, err := c.getClientAuth()
	if err != nil {
		return nil, e.W(err, ECode040301)
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
			cust, err = c.CartGetCustomerByUsername(storeCode, iu.Username)
			if err != nil {
				return nil, e.W(err, ECode040302)
			}

			// Try to upsert with the id now
			iu.ID = cust.ID
			cust, err = c.RegisterCartCustomer(storeCode, iu, password, false)
			if err != nil {
				return nil, e.W(err, ECode040303)
			}
		} else {
			return nil, e.W(err, ECode040304)
		}
	} else {
		cust = &model.CoreUser{}
		if err := json.Unmarshal(res.Data, cust); err != nil {
			return nil, e.W(err, ECode040305)
		}
	}

	return cust, nil
}

// CartGetCustomerByUsername fetches the customer by username from the specified store
func (c *Client) CartGetCustomerByUsername(storeCode, username string) (
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

	ca, err := c.getClientAuth()
	if err != nil {
		return nil, e.W(err, ECode040306)
	}
	res, err := c.sendSingleRequestItem(
		c.deployment.getManageCartServiceURL(storeCode),
		ri,
		ca)
	if err != nil {
		return nil, e.W(err, ECode040307)
	}

	custList := []*model.CoreUser{}
	if err := json.Unmarshal(res.Data, &custList); err != nil {
		return nil, e.W(err, ECode040308)
	}

	if len(custList) != 1 {
		return nil, fmt.Errorf(e.MsgCartCustomerNotExists)
	}

	return custList[0], nil
}
