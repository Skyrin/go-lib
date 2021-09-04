package arc

import (
	"encoding/json"
	"fmt"

	gle "github.com/Skyrin/go-lib/errors"
)

// CartCustomerInput
type CartCustomer struct {
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

type CorePerson struct {
	FirstName  string `json:"firstName"`
	MiddleName string `json:"middleName"`
	LastName   string `json:"lastName"`
}

// CartUpsertCustomer makes an upsert cart customer call to Arc
func (c *Client) CartUpsertCustomer(storeCode string,
	ci *CartCustomer) (cust *CartCustomer, err error) {

	var params []interface{}
	if ci.ArcUserID > 0 {
		params = append(params, ci.ArcUserID)
	} else {
		params = append(params, nil)
	}

	rio := RequestItemOption{}
	rio.Value = map[string]interface{}{}
	rio.Value["username"] = ci.Email
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
		return nil, gle.Wrap(err, "CartUpsertCustomer.1", "")
	}
	res, err := c.sendSingleRequestItem(
		c.deployment.getManageCartServiceURL(storeCode),
		ri,
		ca)
	if err != nil {
		if res != nil && res.ErrorCode == "E01FAAE" {
			// User already exists in the system, return specific error
			return nil, fmt.Errorf(ErrCartCustomerExists)
		} else {
			return nil, gle.Wrap(err, "CartUpsertCustomer.2", "")
		}
	}

	cust = &CartCustomer{}
	if err := json.Unmarshal(res.Data, cust); err != nil {
		return nil, gle.Wrap(err, "CartUpsertCustomer.3", "")
	}

	return cust, nil
}

// CartGetCustomerByUsername fetches the customer by username from the specified store
func CartGetCustomerByUsername(c *Client, storeCode, username string) (cust *CartCustomer, err error) {
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
		return nil, gle.Wrap(err, "CartUpsertCustomer.1", "")
	}
	res, err := c.sendSingleRequestItem(
		c.deployment.getManageCartServiceURL(storeCode),
		ri,
		ca)
	if err != nil {
		return nil, gle.Wrap(err, "CartGetCustomer.2", "")
	}

	custList := []*CartCustomer{}
	if err := json.Unmarshal(res.Data, &custList); err != nil {
		return nil, gle.Wrap(err, "CartGetCustomer.3", "")
	}

	if len(custList) != 1 {
		return nil, fmt.Errorf(ErrCartCustomerNotExists)
	}

	return custList[0], nil
}
