package model

// SyncItem model
type SyncItem struct {
	ID        int
	ItemID    int
	Item      []byte
	ItemHash  string
	CreatedOn string
	UpdatedOn string
}

func (si *SyncItem) MarshalJSON() ([]byte, error) {
	return si.Item, nil
}
