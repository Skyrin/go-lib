package sqlmodel

import (
	"fmt"

	"github.com/Skyrin/go-lib/e"
	"github.com/Skyrin/go-lib/pubsub/model"
	"github.com/Skyrin/go-lib/sql"
)

const (
	// Error constants
	ECode070501 = e.Code0705 + "01"
	ECode070502 = e.Code0705 + "02"
)

// SubDataBulk optimized way to mark records as completed
type SubDataBulk struct {
	bu *sql.BulkUpdate
}

// NewSubDataBulk initializes and returns a new bulk updater for marking sub data as complete.
// It sets the status to complete, the hash and the json bytes if present
// Note: whatever calls this must call Flush and Close when done
func NewSubDataBulk(db *sql.Connection) (sdbc *SubDataBulk) {
	sdbc = &SubDataBulk{}
	sdbc.bu, _ = sql.NewBulkUpdate(db, SubDataTableName, []sql.BulkUpdateCol{
		{Name: "dps_sub_data_id", Type: "BIGINT"},
		{Name: "dps_sub_data_status", Type: "t_skyrin_dps_sub_data_status"},
		{Name: "dps_sub_data_hash", Type: "TEXT"},
		{Name: "dps_sub_data_json", Type: "JSONB"},
		{Name: "dps_sub_data_retries", Type: "INT"},
		{Name: "dps_sub_data_message", Type: "TEXT"},
	}, []string{
		"dps_sub_data_id",
	}, true)

	return sdbc
}

// Add adds the item to the bulk insert. If it saves to the database, it will return the
// number of rows added
func (b *SubDataBulk) Add(sd *model.SubData) (rowsUpdated int, err error) {
	var jsonBytes interface{}
	if sd.JSON != nil {
		jsonBytes = sd.JSON
	}
	rowsUpdated, err = b.bu.Add(sd.ID,
		sd.Status, sd.Hash, jsonBytes,
		sd.Retries, sd.Message)
	if err != nil {
		return 0, e.W(err, ECode070501,
			fmt.Sprintf("id: %d, pubId: %d, data type: %s, data id: %s, hash: %s",
				sd.ID, sd.PubID, sd.Type, sd.DataID, sd.Hash))
	}

	return rowsUpdated, nil
}

// Flush saves any pending records
func (b *SubDataBulk) Flush() (err error) {
	if err = b.bu.Flush(); err != nil {
		return e.W(err, ECode070502)
	}

	return nil
}

// Close closes all open statements
func (b *SubDataBulk) Close() (errList []error) {
	return b.bu.Close()
}

// GetCount returns the number of rows added since the last flush call
func (b *SubDataBulk) GetCount() (count int) {
	return b.bu.GetCount()
}

// GetTotal returns the total number of rows added
func (b *SubDataBulk) GetTotal() (count int) {
	return b.bu.GetTotal()
}
