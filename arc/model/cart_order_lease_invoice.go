package model

// CartOrderLeaseInvoice
type CartOrderLeaseInvoice struct {
	ID            int     `json:"id"`
	LeaseID       int     `json:"leaseId"`
	Note          string  `json:"note"`
	Interval      int     `json:"interval"`
	DueTime       int     `json:"dueTime"`
	DueAmount     float64 `json:"dueAmount"`
	PastDueAmount float64 `json:"pastDueAmount"`
	PaidAmount    float64 `json:"paidAmount"`
	StatusCode    string  `json:"statusCode"`
	ErrorText     string  `json:"errorText"`
}
