package sync

import (
	"github.com/Skyrin/go-lib/e"
	"github.com/Skyrin/go-lib/sql"
	"github.com/Skyrin/go-lib/sync/model"
	"github.com/Skyrin/go-lib/sync/sqlmodel"
)

// SyncUpsert performs the DB operation to upsert a record in the sync_queue
func SyncUpsert(db *sql.Connection, itemID int, input []*model.SyncQueue) (err error) {
	// Start Tx
	tx, err := db.BeginReturnDB()
	if err != nil {
		return e.Wrap(err, e.Code060D, "01")
	}
	defer db.RollbackIfInTxn()

	for _, i := range input {
		// Save to the sync_queue table for each service
		_, err = sqlmodel.SyncQueueUpsert(tx, i)
		if err != nil {
			return e.Wrap(err, e.Code060D, "02")
		}
	}

	// Commit
	if err := tx.Commit(); err != nil {
		return e.Wrap(err, e.Code060D, "03")
	}

	return nil
}
