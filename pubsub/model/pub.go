package model

import "time"

const (
	PubStatusActive   = "active"
	PubStatusInactive = "inactive"
)

// Pub model
type Pub struct {
	ID        int
	Code      string
	Name      string
	Status    string
	CreatedOn time.Time
	UpdatedOn time.Time
}

// ScanPointers returns a list of pointers to the properties that should
// be used when scanning the fields from the database
func (s *Pub) ScanPointers() (scanList []interface{}) {
	return []interface{}{
		&s.ID, &s.Code, &s.Name, &s.Status,
		&s.CreatedOn, &s.UpdatedOn,
	}
}

// InsertValues converts the struct into a slice of insert values
func (s *Pub) InsertValues() (v []interface{}, err error) {
	v = []interface{}{
		s.Code, s.Name, s.Status,
		"now()", "now()",
	}

	return v, nil
}
