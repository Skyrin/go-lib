package model

import "encoding/json"

// CartOrderLease
type CartOrderLease struct {
	ID                  int                   `json:"id"`
	OrderID             int                   `json:"orderId"`
	UserID              int                   `json:"userId"`
	RentalAssetID       int                   `json:"rentalAssetId"`
	PeriodID            int                   `json:"periodId"`
	Interval            int                   `json:"interval"`
	StatusCode          string                `json:"statusCode"`
	Price               float64               `json:"price"`
	Deposit             float64               `json:"deposit"`
	StartTime           int                   `json:"startTime"`
	EndTime             int                   `json:"endTime"`
	Cancellable         int                   `json:"cancellableTime"`
	LastInterval        int                   `json:"lastInterval"`
	NextYearMonth       int                   `json:"nextYearMonth"`
	DueDay              int                   `json:"dueDay"`
	AutoRenewStatusCode string                `json:"autoRenewStatusCode"`
	Properties          json.RawMessage       `json:"properties"`
	RentalAsset         CartRentalAsset       `json:"asset"`
	Period              CorePeriod            `json:"period"`
	Invoice             CartOrderLeaseInvoice `json:"invoice"`
	Order               CartOrder             `json:"order"`
}
