package model

import "time"

const (
	ProcessStatusReady   = "ready"
	ProcessStatusRunning = "running"
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
