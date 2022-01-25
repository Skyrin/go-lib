package sync

import (
	"github.com/Skyrin/go-lib/e"
	"github.com/Skyrin/go-lib/sql"
	"github.com/Skyrin/go-lib/sync/model"
	"github.com/Skyrin/go-lib/sync/sqlmodel"
)

const (
	ECode060201 = e.Code0602 + "01"
	ECode060202 = e.Code0602 + "02"
	ECode060203 = e.Code0602 + "03"
)

// SyncUpsert performs the DB operation to upsert a record in the sync_queue
func SyncUpsert(db *sql.Connection, itemID int, input []*model.SyncQueue) (err error) {
	// Start Tx
	tx, err := db.BeginReturnDB()
	if err != nil {
		return e.W(err, ECode060201)
	}
	defer db.RollbackIfInTxn()

	for _, i := range input {
		// Save to the sync_queue table for each service
		_, err = sqlmodel.Upsert(tx, i)
		if err != nil {
			return e.W(err, ECode060202)
		}
	}

	// Commit
	if err := tx.Commit(); err != nil {
		return e.W(err, ECode060203)
	}

	return nil
}
