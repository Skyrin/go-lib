package pubsub

import (
	"fmt"
	"strconv"
	"strings"

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
	ECode070104 = e.Code0701 + "04"
	ECode070105 = e.Code0701 + "05"
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

// PublishList upserts the list of new pub data records. If any already exists, it will
// update the deleted field, the JSON value and increment the version for that record. If
// duplicate records are in the list, it is not guaranteed which one will be saved. This would
// only have an impact if the deleted value or the JSON value are different between the
// duplicate records.
func PublishList(db *sql.Connection, list []PublishParam) (successCount int, err error) {
	bu := sqlmodel.NewDataBulkInsert(db)

	// Track and ignore duplicate records
	duplicates := make(map[string]*model.Data, len(list))
	for i := range list {
		id := strings.Join([]string{strconv.Itoa(list[i].PublishID), list[i].Type, list[i].ID}, ".")

		if list[i].Type == "" {
			return 0, e.N(ECode070101, fmt.Sprintf("idx %d missing type", i))
		}
		if list[i].ID == "" {
			return 0, e.N(ECode070102, fmt.Sprintf("idx %d missing id", i))
		}

		if duplicates[id] != nil {
			// Already in the list, skip it
			continue
		}

		d := &model.Data{
			PubID:   list[i].PublishID,
			Type:    list[i].Type,
			ID:      list[i].ID,
			Deleted: list[i].Deleted,
			JSON:    list[i].JSON,
		}
		duplicates[id] = d
		rowsUpdated, err := bu.Add(d)
		if err != nil {
			return successCount, e.W(err, ECode070104)
		}
		if rowsUpdated > 0 {
			// Reset duplicates since records were saved
			duplicates = make(map[string]*model.Data, 0)
		}
		successCount += rowsUpdated
	}

	if err := bu.Flush(); err != nil {
		return successCount, e.W(err, ECode070105)
	}

	return successCount, nil
}
