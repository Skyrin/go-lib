package model

import (
	"fmt"
)

const (
	AlgoliaSyncStatusPending  = "pending"
	AlgoliaSyncStatusFailed   = "failed"
	AlgoliaSyncStatusComplete = "complete"
)

// AlgoliaSync model
type AlgoliaSync struct {
	ID              int
	AlgoliaIndex    string
	AlgoliaObjectID string
	ItemID          int
	ItemType        string
	Item            []byte
	ItemHash        string
	Status          string
	ForDelete       bool
	CreatedOn       string
	UpdatedOn       string
}

// CreateAlgoliaObjectID returns a string that represents the algolia object id
// Uniqueness comes from the item type/item id combination. If a non-empty string
// is provided as the prefix, it will be added to the begining of the generated
// id.
func CreateAlgoliaObjectID(prefix string, itemType string, id int) string {
	if prefix != "" {
		return fmt.Sprintf("%s_%s_%d", prefix, itemType, id)
	}

	return fmt.Sprintf("%s_%d", itemType, id)
}

func (ags *AlgoliaSync) MarshalJSON() ([]byte, error) {
	return ags.Item, nil
}
