package model

import "time"

const (
	ProcessStatusActive   = "active"
	ProcessStatusInactive = "inactive"
)

type Process struct {
	ID          int
	Code        string
	Name        string
	LastRunTime time.Time
	NextRunTime time.Time
	Interval    time.Duration
	Status      string
	Message     string
	CreatedOn   time.Time
	UpdatedOn   time.Time
}
