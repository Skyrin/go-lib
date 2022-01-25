package model

// CorePeriod
type CorePeriod struct {
	ID               int    `json:"id"`
	Code             string `json:"code"`
	Name             string `json:"name"`
	IntervalTypeCode string `json:"intervalTypeCode"`
	IntervalMax      int    `json:"intervalMax"`
}
