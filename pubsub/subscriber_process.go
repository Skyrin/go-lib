package pubsub

import (
	"github.com/Skyrin/go-lib/e"
	"github.com/Skyrin/go-lib/pubsub/internal/sqlmodel"
)

const (
	// Error constants
	ECode070401 = e.Code0704 + "01"
	ECode070402 = e.Code0704 + "02"
)

func (s *Subscriber) createMissingAndUpdateExisting() (err error) {
	// Create any new records from the skyrin_dps_data table
	if err := sqlmodel.SubDataCreateMissing(s.db, s.sub.ID); err != nil {
		return e.W(err, ECode070401)
	}

	// Update deleted/version for existing records
	if err := sqlmodel.SubDataUpdateFromPub(s.db, s.sub.ID); err != nil {
		return e.W(err, ECode070402)
	}

	return nil
}
