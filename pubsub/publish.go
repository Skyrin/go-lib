package pubsub

import (
	"github.com/Skyrin/go-lib/e"
	"github.com/Skyrin/go-lib/pubsub/internal/sqlmodel"
	"github.com/Skyrin/go-lib/pubsub/model"
	"github.com/Skyrin/go-lib/sql"
)

const (
	// Error constants
	ECode070101 = e.Code0701 + "01"
	ECode070102 = e.Code0701 + "02"
	ECode070103 = e.Code0701 + "03"
)

// PublishParam parameters for the Publish func
type PublishParam struct {
	PublishID int    // The publisher id
	Type      string // The data type
	ID        string // The data id
	Deleted   bool   // Indicates if the item was deleted or not
	JSON      []byte // Optional JSON bytes representing the object
}

// Publish upserts a new pub data record for the specified data type/id. If it already exists,
// it will update the deleted field, the JSON value and increment the version.
func Publish(db *sql.Connection, p PublishParam) (version int, err error) {
	if p.Type == "" {
		return 0, e.N(ECode070101, "missing type")
	}
	if p.ID == "" {
		return 0, e.N(ECode070102, "missing id")
	}

	version, err = sqlmodel.DataUpsert(db, &model.Data{
		PubID:   p.PublishID,
		Type:    p.Type,
		ID:      p.ID,
		Deleted: p.Deleted,
		JSON:    p.JSON,
	})
	if err != nil {
		return 0, e.W(err, ECode070103)
	}

	return version, nil
}
