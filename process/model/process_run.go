package model

import "time"

const (
	ProcessRunStatusRunning   = "running"
	ProcessRunStatusCompleted = "completed"
	ProcessRunStatusFailed    = "failed"
)

type ProcessRun struct {
	ID        uint
	ProcessID uint
	Status    string
	Error     string
	CreatedOn time.Time
	UpdatedOn time.Time
}
