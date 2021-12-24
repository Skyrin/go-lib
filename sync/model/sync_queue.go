package model

const (
	SyncQueueStatusPending  = "pending"
	SyncQueueStatusFailed   = "failed"
	SyncQueueStatusComplete = "complete"
)

// SyncQueue model
type SyncQueue struct {
	ID           int
	Index        string
	ObjectID     string
	SyncItemID   int
	SyncItemType string
	Status       string
	Service      string
	Retries      int
	ForDelete    bool
	CreatedOn    string
	UpdatedOn    string
}
