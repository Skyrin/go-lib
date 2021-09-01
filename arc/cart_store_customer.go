package arc

import (
	"encoding/json"
	"fmt"

	gle "github.com/Skyrin/go-lib/errors"
)

var ErrorCartCustomerExists = fmt.Errorf("Customer Alreay Exists")

// CartUpsertCustomer makes an upsert cart customer call to Arc
func CartStoreCustomerLogin(c *Client, username, password string) (g *Oauth2Grant, err error) {

	if c.deployment.Store == nil {
		return nil, fmt.Errorf(ErrCartStoreNotSet)
	}

	params := []interface{}{
		c.deployment.Store.ClientID,
		username,
		password,
	}

	ri := &RequestItem{
		Service: "core",
		Action:  "oauth2.Grant.login",
		Params:  params,
	}

	res, err := c.sendSingleRequestItem(
		c.deployment.getManageCoreServiceURL(),
		ri,
		true)
	if err != nil {
		return nil, gle.Wrap(err, "CartStoreCustomerLogin.1", "")
	}

	g = &Oauth2Grant{}
	if err := json.Unmarshal(res.Data, g); err != nil {
		return nil, gle.Wrap(err, "CartStoreCustomerLogin.3", "")
	}

	return g, nil
}
