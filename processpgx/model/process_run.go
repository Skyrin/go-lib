package model

import "time"

const (
	ProcessRunStatusRunning   = "running"
	ProcessRunStatusCompleted = "completed"
	ProcessRunStatusFailed    = "failed"
)

type ProcessRun struct {
	ID        int
	ProcessID int
	Status    string
	RunTime   time.Duration
	Error     string
	CreatedOn time.Time
	UpdatedOn time.Time
}
