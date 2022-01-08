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
	Hash      [32]byte        `json:"-"`
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

// IsValidStatus validates the status
func (d *Data) IsValidType() bool {
	switch d.Type {
	case DataTypeCategory:
		return true
	case DataTypeCustomer:
		return true
	case DataTypeCustomerList:
		return true
	case DataTypeProduct:
		return true
	case DataTypeProductList:
		return true
	case DataTypePurchase:
		return true
	case DataTypeRentalAsset:
		return true
	case DataTypeRentalAssetList:
		return true
	case DataTypeUser:
		return true
	}

	return false
}
