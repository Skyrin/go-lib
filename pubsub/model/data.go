package model

import "time"

const (
	DataStatusPending   = "pending"
	DataStatusFailed    = "failed"
	DataStatusCompleted = "completed"
)

// Data model
type Data struct {
	PubID     int
	Type      string
	ID        string
	Deleted   bool
	Version   int
	JSON      []byte
	CreatedOn time.Time
	UpdatedOn time.Time
}

// ScanPointers returns a list of pointers to the properties that should
// be used when scanning the fields from the database
func (d *Data) ScanPointers() (scanList []interface{}) {
	return []interface{}{
		// &d.ID,
		&d.PubID, &d.Type, &d.ID,
		&d.Deleted, &d.Version, &d.JSON,
		&d.CreatedOn, &d.UpdatedOn,
	}
}

// InsertValues converts the struct into a slice of insert values
func (d *Data) InsertValues() (v []interface{}, err error) {
	v = []interface{}{
		d.PubID, d.Type, d.ID,
		d.Deleted, d.Version, d.JSON,
		"now()", "now()",
	}

	// If nil, insert nil value to DB
	if d.JSON == nil {
		v[5] = nil
	}

	return v, nil
}
