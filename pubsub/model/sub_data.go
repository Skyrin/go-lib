package model

const (
	SubDataStatusPending   = "pending"
	SubDataStatusFailed    = "failed"
	SubDataStatusCompleted = "completed"
)

// SubData model
type SubData struct {
	ID        int
	SubID     int
	PubID     int
	Type      string
	DataID    string
	Deleted   bool
	Version   int
	Status    string
	Hash      string
	JSON      []byte
	Retries   int
	Message   string
	CreatedOn string
	UpdatedOn string
}

// MarshalJSON If we add the item, do not forget to custom marshal JSON for the item
func (sd *SubData) MarshalJSON() ([]byte, error) {
	return sd.JSON, nil
}

// ScanPointers returns a list of pointers to the properties that should
// be used when scanning the fields from the database
func (sd *SubData) ScanPointers() (scanList []interface{}) {
	return []interface{}{
		&sd.ID, &sd.SubID, &sd.PubID, &sd.Type, &sd.DataID,
		&sd.Deleted, &sd.Version, &sd.Status,
		&sd.Hash, &sd.JSON,
		&sd.Retries, &sd.Message,
		&sd.CreatedOn, &sd.UpdatedOn,
	}
}

// InsertValues converts the struct into a slice of insert values
func (sd *SubData) InsertValues() (v []interface{}, err error) {
	v = []interface{}{
		sd.SubID, sd.PubID, sd.Type, sd.DataID,
		sd.Deleted, sd.Version, sd.Status,
		sd.Hash, sd.JSON,
		sd.Retries, sd.Message,
		"now()", "now()",
	}

	// If nil, insert nil value to DB
	if sd.JSON == nil {
		v[8] = nil
	}

	return v, nil
}

// SetResponse updates the sub data with the response from the sub data handler. If
// there was no error returned by the handler, it assigns the new hash, json and sets the status
// of the sub data model to completed. If there was an error, it increments the retries.
// If the retry limit is reached, as defined by the subscriber, then it sets the status
// to failed and if the error handler is set, calls it.
func (sd *SubData) SetResponse(newHash string, newJSON []byte,
	err error, newVersion int, newDeleted bool, s *Sub) {

	if err != nil {
		sd.SetError(err, s)
	} else {
		sd.Status = SubDataStatusCompleted
	}

	// Set new hash, json, deleted and version
	sd.Hash = newHash
	sd.JSON = newJSON
	sd.Version = newVersion
	sd.Deleted = newDeleted
}

// SetError sets the error message for the sub data. If the retries for this record is less than the
// sub's configured retries, then it increments the retries only. Else, it sets the status to failed and
// resets the retry count.
func (sd *SubData) SetError(err error, s *Sub) {
	if err == nil {
		return
	}

	sd.Message = err.Error()
	if sd.Retries < s.Retries {
		// Ensure the status remains 'pending'
		sd.Status = SubDataStatusPending

		// Increment the retries, set the message as the error and continue for now
		sd.Retries++
	} else {
		// Mark as failed and reset retries
		sd.Status = SubDataStatusFailed
		sd.Retries = 0
	}
}
