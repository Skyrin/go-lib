package model

import "time"

const (
	SubStatusActive   = "active"
	SubStatusInactive = "inactive"
)

// Sub model
type Sub struct {
	ID        int
	Code      string
	Name      string
	Status    string
	Retries   int
	CreatedOn time.Time
	UpdatedOn time.Time
}

// ScanPointers returns a list of pointers to the properties that should
// be used when scanning the fields from the database
func (s *Sub) ScanPointers() (scanList []interface{}) {
	return []interface{}{
		&s.ID, &s.Code, &s.Name, &s.Status, &s.Retries,
		&s.CreatedOn, &s.UpdatedOn,
	}
}

// InsertValues converts the struct into a slice of insert values
func (s *Sub) InsertValues() (v []interface{}, err error) {
	v = []interface{}{
		s.Code, s.Name, s.Status, s.Retries,
		"now()", "now()",
	}

	return v, nil
}
