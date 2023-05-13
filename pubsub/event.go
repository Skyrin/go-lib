package pubsub

import (
	"github.com/Skyrin/go-lib/e"
	"github.com/Skyrin/go-lib/pubsub/internal/sqlmodel"
	"github.com/Skyrin/go-lib/pubsub/model"
	"github.com/Skyrin/go-lib/sql"
)

// Event the expected JSON from a skyrin_dps_notify call
type Event struct {
	PubID        int            `json:"pubId"`
	Type         string         `json:"dataType"`
	ID           string         `json:"dataId"`
	Deleted      bool           `json:"deleted"`
	Version      int            `json:"version"`
	PreviousHash string         `json:"-"`
	NewHash      string         `json:"-"`
	NewJSON      []byte         `json:"-"`
	sd           *model.SubData `json:"-"`
	err          error          `json:"-"`
}

// GetEventJSON retrieves the new JSON from the event record
func (ev *Event) GetEventJSON(db *sql.Connection) (b []byte, err error) {
	d, err := sqlmodel.DataGetByPubIDDataTypeAndDataID(db, ev.PubID, ev.Type, ev.ID)
	if err != nil {
		return nil, e.W(err, ECode07030A)
	}

	return d.JSON, nil
}

// Error sets an error for the event. If an error is set for an event, it will be automatically
// saved with the event when the status is updated
func (ev *Event) Error(err error) {
	ev.err = err
}
