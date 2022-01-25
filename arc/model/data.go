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

	DataStatusPending    = DataStatus("pending")
	DataStatusProcessing = DataStatus("processing")
	DataStatusProcessed  = DataStatus("processed")

	DataTypeCategory = DataType("category")
	// DataTypeCategoryList    = DataType("category-list")
	DataTypeCustomer        = DataType("customer")
	DataTypeCustomerList    = DataType("customer-list")
	DataTypeOrderLease      = DataType("order-lease")
	DataTypeProduct         = DataType("product")
	DataTypeProductList     = DataType("product-list")
	DataTypePurchase        = DataType("purchase")
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
	Hash      []byte          `json:"-"`
	CreatedOn time.Time       `json:"-"`
	UpdatedOn time.Time       `json:"-"`
}

// IsValidAppCode validates the app code
func (d *Data) IsValidAppCode() bool {
	switch d.AppCode {
	case AppCodeCore:
		return true
	case AppCodeCart:
		return true
	case AppCodeArcimedes:
		return true
	}

	return false
}
