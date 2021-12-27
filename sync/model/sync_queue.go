package model

const (
	SyncQueueStatusPending  = "pending"
	SyncQueueStatusFailed   = "failed"
	SyncQueueStatusComplete = "complete"
)

// SyncQueue model
type SyncQueue struct {
	ID        int
	Index     string
	ObjectID  string
	ItemID    int
	Item      *[]byte
	ItemHash  string
	ItemType  string
	Status    string
	Service   string
	Retries   int
	ForDelete bool
	CreatedOn string
	UpdatedOn string
}

// If we add the item, do not forget to custom marshal JSON for the item
// func (sq *SyncQueue) MarshalJSON() ([]byte, error) {
// 	return sq.Item, nil
// }
