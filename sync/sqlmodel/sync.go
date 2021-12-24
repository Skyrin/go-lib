package sqlmodel

import (
	"github.com/Skyrin/go-lib/e"
	"github.com/Skyrin/go-lib/sql"
	"github.com/Skyrin/go-lib/sync/model"
)

// SyncUpsert performs the DB operation to upsert a record in the sync_queue and sync_item table
func SyncUpsert(db *sql.Connection, itemID int, input []*model.SyncQueue) (err error) {
	// Start Tx
	tx, err := db.BeginReturnDB()
	if err != nil {
		return e.Wrap(err, e.Code060D, "01")
	}
	defer db.RollbackIfInTxn()

	si := &model.SyncItem{
		ItemID:   itemID,
		Item:     nil,
		ItemHash: "",
	}

	// Save to the sync_item table
	syncItemID, err := SyncItemAdd(tx, si)
	if err != nil {
		return e.Wrap(err, e.Code060D, "02")
	}

	for _, i := range input {
		i.SyncItemID = syncItemID
		// Save to the sync_queue table for each service
		_, err = SyncQueueUpsert(tx, i)
		if err != nil {
			return e.Wrap(err, e.Code060D, "03")
		}
	}

	// Commit
	if err := tx.Commit(); err != nil {
		return e.Wrap(err, e.Code060D, "04")
	}

	return nil
}
