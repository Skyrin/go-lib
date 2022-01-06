package model

import (
	"encoding/json"
	"time"
)

type AppCode string
type DataType string
type DataStatus string

const (
	AppCodeArcimedes = AppCode("arcimedes")
	AppCodeCart      = AppCode("cart")
	AppCodeCore      = AppCode("core")

	DataStatusError     = DataStatus("error")
	DataStatusPending   = DataStatus("pending")
	DataStatusProcessed = DataStatus("processed")

	DataTypeCategory = DataType("category")
	// DataTypeCategoryList    = DataType("category-list")
	DataTypeCustomer        = DataType("customer")
	DataTypeCustomerList    = DataType("customer-list")
	DataTypeProduct         = DataType("product")
	DataTypeProductList     = DataType("product-list")
	DataTypeRentalAsset     = DataType("rental-asset")
	DataTypeRentalAssetList = DataType("rental-asset-list")
	DataTypeUser            = DataType("user")
)

// Deployment
type Data struct {
	AppCode   AppCode         `json:"appCode"`
	AppCoreID uint            `json:"appCoreId"`
	Type      DataType        `json:"objectType"`
	ObjectID  uint            `json:"objectId"`
	Status    DataStatus      `json:"status"`
	Object    json.RawMessage `json:"object"`
	Deleted   bool            `json:"deleted"`
	Hash      [32]byte        `json:"-"`
	CreatedOn time.Time       `json:"-"`
	UpdatedOn time.Time       `json:"-"`
}
