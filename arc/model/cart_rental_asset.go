package model

import "encoding/json"

// CartRentalAsset
type CartRentalAsset struct {
	ID     int             `json:"id"`
	Code   string          `json:"code"`
	Name   string          `json:"name"`
	Desc   string          `json:"desc"`
	Status string          `json:"statusCode"`
	Ext    json.RawMessage `json:"_ext"`
}
