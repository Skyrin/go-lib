package sqlmodel

import (
	"fmt"
	"strings"

	"github.com/Skyrin/go-lib/algolia/model"
	"github.com/Skyrin/go-lib/e"
	"github.com/Skyrin/go-lib/sql"
	"github.com/lib/pq"
)

const (
	AlgoliaSyncTableName = "algolia_sync"
	AlgoliaDefaultSortBy = "algolia_sync_id"

	ECode050301 = e.Code0503 + "01"
	ECode050302 = e.Code0503 + "02"
	ECode050303 = e.Code0503 + "03"
	ECode050304 = e.Code0503 + "04"
	ECode050305 = e.Code0503 + "05"
	ECode050306 = e.Code0503 + "06"
	ECode050307 = e.Code0503 + "07"
	ECode050308 = e.Code0503 + "08"
	ECode050309 = e.Code0503 + "09"
	ECode05030A = e.Code0503 + "0A"
	ECode05030B = e.Code0503 + "0B"
	ECode05030C = e.Code0503 + "0C"
)

// AlgoliaSyncGetParam model
type AlgoliaSyncGetParam struct {
	Limit                  *uint64
	Offset                 *uint64
	ID                     *int
	AlgoliaObjectID        *string
	ItemID                 *int
	ItemType               *string
	Status                 *[]string
	AlgoliaIndex           *string
	ForDelete              *bool
	FlagCount              bool
	FlagForUpdate          bool
	OrderByID              string
	OrderByAlgoliaObjectID string
	DataHandler            func(*model.AlgoliaSync) error
}

// AlgoliaSyncUpsert performs the DB operation to upsert a record in the algolia_sync table
func AlgoliaSyncUpsert(db *sql.Connection, input *model.AlgoliaSync) (id int, err error) {
	ib := db.Insert(AlgoliaSyncTableName).
		Columns(`algolia_sync_index, algolia_sync_object_id, algolia_sync_item_id, algolia_sync_item, 
			algolia_sync_item_hash, algolia_sync_status, algolia_sync_item_type,
			created_on, updated_on`).
		Values(input.AlgoliaIndex, input.AlgoliaObjectID, input.ItemID, input.Item,
			input.ItemHash, input.Status, input.ItemType,
			"now()", "now()").
		Suffix(`ON CONFLICT (algolia_sync_index, algolia_sync_object_id) DO UPDATE
			SET algolia_sync_item_id=excluded.algolia_sync_item_id, 
			algolia_sync_item=excluded.algolia_sync_item,
			algolia_sync_item_hash=excluded.algolia_sync_item_hash,
			algolia_sync_status=excluded.algolia_sync_status,
			algolia_sync_item_type=excluded.algolia_sync_item_type,
			updated_on=now()
			RETURNING algolia_sync_id`)

	id, err = db.ExecInsertReturningID(ib)
	if err != nil {
		return 0, e.W(err, ECode050301)
	}

	return id, nil
}

// AlgoliaSyncSetStatus updates the status. If hash or jsonBytes are set, then
// it will update those as well
func AlgoliaSyncSetStatus(db *sql.Connection, id int, status string,
	hash *string, jsonBytes []byte) (err error) {
	ub := db.Update(AlgoliaSyncTableName).
		Where("algolia_sync_id=?", id).
		Set("algolia_sync_status", status).
		Set("updated_on", "now()")

	if hash != nil {
		ub = ub.Set("algolia_sync_item_hash", *hash)
	}

	if jsonBytes != nil {
		ub = ub.Set("algolia_sync_item", jsonBytes)
	}

	if err := db.ExecUpdate(ub); err != nil {
		return e.W(err, ECode050302)
	}

	return nil
}

// AlgoliaSyncForDeleteUpdate updates the delete flag
func AlgoliaSyncForDeleteUpdate(db *sql.Connection, id int, delete bool) (err error) {
	ub := db.Update(AlgoliaSyncTableName).
		Where("algolia_sync_id=?", id).
		Set("algolia_sync_item_delete", delete).
		Set("updated_on", "now()")

	if delete {
		ub = ub.Set("algolia_sync_status", model.AlgoliaSyncStatusPending)
	}

	if err := db.ExecUpdate(ub); err != nil {
		return e.W(err, ECode050303)
	}

	return nil
}

// AlgoliaSyncGet performs select
func AlgoliaSyncGet(db *sql.Connection,
	p *AlgoliaSyncGetParam) (asList []*model.AlgoliaSync, count int, err error) {
	fields := `algolia_sync_id, algolia_sync_index, algolia_sync_object_id, algolia_sync_item_id, 
		algolia_sync_item, algolia_sync_item_hash, algolia_sync_status, algolia_sync_item_delete,
		algolia_sync_item_type, created_on, updated_on`

	sb := db.Select("{fields}").
		From(AlgoliaSyncTableName)

	if p.Limit != nil && *p.Limit > 0 {
		sb = sb.Limit(*p.Limit)
	}

	if p.FlagForUpdate {
		sb = sb.Suffix("FOR UPDATE")
	}

	if p.ID != nil && *p.ID >= 0 {
		sb = sb.Where("algolia_sync_id=?", *p.ID)
	}

	if p.ItemID != nil && *p.ItemID >= 0 {
		sb = sb.Where("algolia_sync_item_id=?", *p.ItemID)
	}

	if p.AlgoliaObjectID != nil && len(*p.AlgoliaObjectID) > 0 {
		sb = sb.Where("algolia_sync_object_id=?", *p.AlgoliaObjectID)
	}

	if p.AlgoliaIndex != nil && len(*p.AlgoliaIndex) > 0 {
		sb = sb.Where("algolia_sync_index=?", *p.AlgoliaIndex)
	}

	if p.ItemType != nil && len(*p.ItemType) > 0 {
		sb = sb.Where("algolia_sync_item_type=?", *p.ItemType)
	}

	if p.Status != nil && len(*p.Status) > 0 {
		sb = sb.Where("algolia_sync_status = ANY(?)", pq.Array(*p.Status))
	}

	if p.ForDelete != nil {
		sb = sb.Where("algolia_sync_item_delete=?", *p.ForDelete)
	}

	stmt, bindList, err := sb.ToSql()
	if err != nil {
		return nil, 0, e.W(err, ECode050304)
	}

	if p.FlagCount {
		row := db.QueryRow(strings.Replace(stmt, "{fields}", "count(*)", 1), bindList...)
		if err := row.Scan(&count); err != nil {
			return nil, 0, e.W(err, ECode050305,
				fmt.Sprintf("AlgoliaSyncGet.2 | stmt: %s, bindList: %+v",
					stmt, bindList))
		}
	}

	if p.Offset != nil {
		sb = sb.Offset(uint64(*p.Offset))
	}

	if p.OrderByID != "" {
		sb = sb.OrderBy(fmt.Sprintf("algolia_sync_id %s", p.OrderByID))
	}

	if p.OrderByAlgoliaObjectID != "" {
		sb = sb.OrderBy(fmt.Sprintf("algolia_sync_object_id %s", p.OrderByAlgoliaObjectID))
	}

	stmt, bindList, err = sb.ToSql()
	stmt = strings.Replace(stmt, "{fields}", fields, 1)
	rows, err := db.Query(stmt, bindList...)
	if err != nil {
		return nil, 0, e.W(err, ECode050306)
	}
	defer rows.Close()

	for rows.Next() {
		as := &model.AlgoliaSync{}
		if err := rows.Scan(&as.ID, &as.AlgoliaIndex, &as.AlgoliaObjectID, &as.ItemID, &as.Item,
			&as.ItemHash, &as.Status, &as.ForDelete, &as.ItemType,
			&as.CreatedOn, &as.UpdatedOn); err != nil {
			return nil, 0, e.W(err, ECode050307)
		}

		if p.DataHandler != nil {
			if err := p.DataHandler(as); err != nil {
				return nil, 0, e.W(err, ECode050308)
			}
		} else {
			asList = append(asList, as)
		}
	}

	return asList, count, nil
}

// AlgoliaSyncGetByStatus returns the items with the specified status
func AlgoliaSyncGetByStatus(db *sql.Connection, status []string, limit *uint64) (asList []*model.AlgoliaSync,
	count int, err error) {
	p := &AlgoliaSyncGetParam{
		Status: &status,
		Limit:  limit,
	}

	return AlgoliaSyncGet(db, p)
}

// AlgoliaSyncGetByItemID searches by the item id
func AlgoliaSyncGetByItemIDAndType(db *sql.Connection, itemID int,
	itemType string) (as *model.AlgoliaSync, err error) {

	limit := uint64(1)
	p := &AlgoliaSyncGetParam{
		Limit:    &limit,
		ItemID:   &itemID,
		ItemType: &itemType,
	}

	asList, _, err := AlgoliaSyncGet(db, p)
	if err != nil {
		return nil, e.W(err, ECode050309)
	}

	if len(asList) == 0 {
		return nil, e.W(err, ECode05030A)
	}

	return asList[0], nil
}

// AlgoliaSyncGetItemIDs Get list of all items IDs
func AlgoliaSyncGetItemIDs(db *sql.Connection, limit, offset int, status []string) (idList []int, count int, err error) {
	fields := `algolia_sync_item_id`

	sb := db.Select("{fields}").
		From(AlgoliaSyncTableName).
		OrderBy("algolia_sync_item_id asc").
		Limit(uint64(limit)).
		Offset(uint64(offset))

	if len(status) > 0 {
		sb = sb.Where("algolia_sync_status = ANY(?)", pq.Array(status))
	}

	stmt, bindList, err := sb.ToSql()
	stmt = strings.Replace(stmt, "{fields}", fields, 1)
	rows, err := db.Query(stmt, bindList...)
	if err != nil {
		return nil, 0, e.W(err, ECode05030B)
	}
	defer rows.Close()

	for rows.Next() {
		var id int
		if err := rows.Scan(&id); err != nil {
			return nil, 0, e.W(err, ECode05030C)
		}

		idList = append(idList, id)
	}

	return idList, len(idList), nil
}
