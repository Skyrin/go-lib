package model

import "time"

const (
	ProcessStatusActive = "active"
	ProcessStatusInactive = "inactive"
)

type Process struct {
	ID        int
	Code      string
	Name      string
	Status    string
	Message   string
	CreatedOn time.Time
	UpdatedOn time.Time
}
