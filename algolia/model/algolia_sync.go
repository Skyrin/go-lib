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
	Item            []byte
	ItemHash        string
	Status          string
	ForDelete       bool
	CreatedOn       string
	UpdatedOn       string
}

// CreateAlgoliaObjectID returns a string that represents the algolia object id
// To ensure it is unique we attach a prefix that defines what the object is related to (table name)
// plus the id from our DB for that object
func CreateAlgoliaObjectID(prefix string, id int) string {
	return fmt.Sprintf("%s_%d", prefix, id)
}

func (ags *AlgoliaSync) MarshalJSON() ([]byte, error) {
	return ags.Item, nil
}
