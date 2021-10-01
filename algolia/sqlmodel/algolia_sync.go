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
)

// AlgoliaSyncGetParam model
type AlgoliaSyncGetParam struct {
	Limit                  uint64
	Offset                 uint64
	ID                     *int
	AlgoliaObjectID        *string
	ItemID                 *int
	Status                 *[]string
	FlagCount              bool
	OrderByID              string
	OrderByAlgoliaObjectID string
}

// AlgoliaSyncUpsert performs the DB operation to upsert a record in the algolia_sync table
func AlgoliaSyncUpsert(db *sql.Connection, input *model.AlgoliaSync) (id int, err error) {
	ib := db.Insert(AlgoliaSyncTableName).
		Columns(`algolia_sync_object_id, algolia_sync_item_id, algolia_sync_item, 
			algolia_sync_item_hash, algolia_sync_status, 
			created_on, updated_on`).
		Values(input.AlgoliaObjectID, input.ItemID, input.Item,
			input.ItemHash, input.Status,
			"now()", "now()").
		Suffix(`ON CONFLICT (algolia_sync_object_id) DO UPDATE
			SET algolia_sync_item_id=excluded.algolia_sync_item_id, 
			algolia_sync_item=excluded.algolia_sync_item,
			algolia_sync_item_hash=excluded.algolia_sync_item_hash,
			algolia_sync_status=excluded.algolia_sync_status,
			updated_on=now()
			RETURNING algolia_sync_id`)

	id, err = db.ExecInsertReturningID(ib)
	if err != nil {
		return 0, e.Wrap(err, e.Code0504, "01")
	}

	return id, nil
}

// AlgoliaSyncStatusUpdate updates the status
func AlgoliaSyncStatusUpdate(db *sql.Connection, id int, status string) (err error) {
	ub := db.Update(AlgoliaSyncTableName).
		Where("algolia_sync_id=?", id).
		Set("algolia_sync_status", status).
		Set("updated_on", "now()")

	if err := db.ExecUpdate(ub); err != nil {
		return e.Wrap(err, e.Code0507, "01")
	}

	return nil
}

// AlgoliaSyncGet performs select
func AlgoliaSyncGet(db *sql.Connection,
	p *AlgoliaSyncGetParam) (asList []*model.AlgoliaSync, count int, err error) {
	if p.Limit == 0 {
		p.Limit = 1
	}

	fields := `algolia_sync_id, algolia_sync_object_id, algolia_sync_item_id, 
		algolia_sync_item, algolia_sync_item_hash, algolia_sync_status, 
		created_on, updated_on`

	sb := db.Select("{fields}").
		From(AlgoliaSyncTableName).
		Limit(p.Limit).
		Suffix("FOR UPDATE")

	if p.ID != nil && *p.ID >= 0 {
		sb = sb.Where("algolia_sync_id=?", *p.ID)
	}

	if p.ItemID != nil && *p.ItemID >= 0 {
		sb = sb.Where("algolia_sync_item_id=?", *p.ItemID)
	}

	if p.AlgoliaObjectID != nil && len(*p.AlgoliaObjectID) > 0 {
		sb = sb.Where("algolia_sync_object_id=?", *p.AlgoliaObjectID)
	}

	if p.Status != nil && len(*p.Status) > 0 {
		sb = sb.Where("algolia_sync_status = ANY(?)", pq.Array(*p.Status))
	}

	stmt, bindList, err := sb.ToSql()
	if err != nil {
		return nil, 0, e.Wrap(err, e.Code0503, "01")
	}

	if p.FlagCount {
		row := db.QueryRow(strings.Replace(stmt, "{fields}", "count(*)", 1), bindList...)
		if err := row.Scan(&count); err != nil {
			return nil, 0, e.Wrap(err, e.Code0503, "02",
				fmt.Sprintf("AlgoliaSyncGet.2 | stmt: %s, bindList: %+v",
					stmt, bindList))
		}
	}

	sb = sb.Offset(uint64(p.Offset))

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
		return nil, 0, e.Wrap(err, e.Code0503, "03")
	}
	defer rows.Close()

	for rows.Next() {
		as := &model.AlgoliaSync{}
		if err := rows.Scan(&as.ID, &as.AlgoliaObjectID, &as.ItemID, &as.Item,
			&as.ItemHash, &as.Status, &as.CreatedOn, &as.UpdatedOn); err != nil {
			return nil, 0, e.Wrap(err, e.Code0503, "04")
		}

		asList = append(asList, as)
	}

	return asList, count, nil
}

// AlgoliaSyncGetByStatus returns the items with the specified status
func AlgoliaSyncGetByStatus(db *sql.Connection, status []string) (asList []*model.AlgoliaSync,
	count int, err error) {
	p := &AlgoliaSyncGetParam{
		Status: &status,
	}

	return AlgoliaSyncGet(db, p)
}

// AlgoliaSyncGetByItemID searches by the item id
func AlgoliaSyncGetByItemID(db *sql.Connection, itemID int) (as *model.AlgoliaSync, err error) {
	p := &AlgoliaSyncGetParam{
		ItemID: &itemID,
	}

	asList, _, err := AlgoliaSyncGet(db, p)
	if err != nil {
		return nil, e.Wrap(err, e.Code0506, "01")
	}

	if len(asList) == 0 {
		return nil, e.Wrap(err, e.Code0506, "02")
	}

	return asList[0], nil
}
