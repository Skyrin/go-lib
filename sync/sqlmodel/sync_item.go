package sqlmodel

import (
	"fmt"
	"strings"

	"github.com/Skyrin/go-lib/e"
	"github.com/Skyrin/go-lib/sql"
	"github.com/Skyrin/go-lib/sync/model"
)

const (
	SyncItemTableName     = "sync_item"
	SyncItemDefaultSortBy = "sync_item_id"
)

// SyncItemGetParam model
type SyncItemGetParam struct {
	Limit         *uint64
	Offset        *uint64
	ID            *int
	ItemID        *int
	FlagCount     bool
	FlagForUpdate bool
	OrderByID     string
	OrderByItemID string
	DataHandler   func(*model.SyncItem) error
}

// SyncItemAdd performs the DB operation to upsert a record in the sync_item table
func SyncItemAdd(db *sql.Connection, input *model.SyncItem) (id int, err error) {
	ib := db.Insert(SyncItemTableName).
		Columns(`item_id, item, item_hash,
			created_on, updated_on`).
		Values(input.ItemID, input.Item, input.ItemHash,
			"now()", "now()").
		Suffix("RETURNING sync_item_id")

	id, err = db.ExecInsertReturningID(ib)
	if err != nil {
		return 0, e.Wrap(err, e.Code0607, "01")
	}

	return id, nil
}

// SyncItemUpdate updates a sync item
func SyncItemUpdate(db *sql.Connection, id int, hash *string,
	jsonBytes []byte) (err error) {
	ub := db.Update(SyncItemTableName).
		Where("sync_item_id=?", id).
		Set("updated_on", "now()")

	if hash != nil {
		ub = ub.Set("item_hash", *hash)
	}

	if jsonBytes != nil {
		ub = ub.Set("item", jsonBytes)
	}

	if err := db.ExecUpdate(ub); err != nil {
		return e.Wrap(err, e.Code060G, "01")
	}

	return nil
}

// SyncItemGet performs select
func SyncItemGet(db *sql.Connection,
	p *SyncItemGetParam) (siList []*model.SyncItem, count int, err error) {
	fields := `sync_item_id, item_id, item, item_hash,
		created_on, updated_on`

	sb := db.Select("{fields}").
		From(SyncItemTableName)

	if p.Limit != nil && *p.Limit > 0 {
		sb = sb.Limit(*p.Limit)
	}

	if p.FlagForUpdate {
		sb = sb.Suffix("FOR UPDATE")
	}

	if p.ID != nil && *p.ID >= 0 {
		sb = sb.Where("sync_item_id=?", *p.ID)
	}

	if p.ItemID != nil && *p.ItemID >= 0 {
		sb = sb.Where("item_id=?", *p.ItemID)
	}

	stmt, bindList, err := sb.ToSql()
	if err != nil {
		return nil, 0, e.Wrap(err, e.Code0608, "01")
	}

	if p.FlagCount {
		row := db.QueryRow(strings.Replace(stmt, "{fields}", "count(*)", 1), bindList...)
		if err := row.Scan(&count); err != nil {
			return nil, 0, e.Wrap(err, e.Code0608, "02",
				fmt.Sprintf("SyncQueueGet.2 | stmt: %s, bindList: %+v",
					stmt, bindList))
		}
	}

	if p.Offset != nil {
		sb = sb.Offset(uint64(*p.Offset))
	}

	orderByDefault := true

	if p.OrderByID != "" {
		sb = sb.OrderBy(fmt.Sprintf("sync_item_id %s", p.OrderByID))
		orderByDefault = false
	}

	if p.OrderByItemID != "" {
		sb = sb.OrderBy(fmt.Sprintf("item_id %s", p.OrderByItemID))
		orderByDefault = false
	}

	if orderByDefault {
		sb = sb.OrderBy(fmt.Sprintf("%s %s", SyncItemDefaultSortBy, "asc"))
	}

	stmt, bindList, err = sb.ToSql()
	stmt = strings.Replace(stmt, "{fields}", fields, 1)
	rows, err := db.Query(stmt, bindList...)
	if err != nil {
		return nil, 0, e.Wrap(err, e.Code0608, "03")
	}
	defer rows.Close()

	for rows.Next() {
		si := &model.SyncItem{}
		if err := rows.Scan(&si.ID, &si.ItemID, &si.Item, &si.ItemHash,
			&si.CreatedOn, &si.UpdatedOn); err != nil {
			return nil, 0, e.Wrap(err, e.Code0608, "04")
		}

		siList = append(siList, si)
	}

	return siList, count, nil
}

// SyncItemGetByID searches by the sync item id
func SyncItemGetByID(db *sql.Connection, id int) (si *model.SyncItem, err error) {
	limit := uint64(1)
	p := &SyncItemGetParam{
		Limit: &limit,
		ID:    &id,
	}

	siList, _, err := SyncItemGet(db, p)
	if err != nil {
		return nil, e.Wrap(err, e.Code0609, "01")
	}

	if len(siList) == 0 {
		return nil, e.Wrap(fmt.Errorf("no items found"), e.Code0609, "02")
	}

	return siList[0], nil
}

// SyncItemGetByItemID searches by the item id
func SyncItemGetByItemID(db *sql.Connection, itemID int) (si *model.SyncItem, err error) {
	limit := uint64(1)
	p := &SyncItemGetParam{
		Limit:  &limit,
		ItemID: &itemID,
	}

	siList, _, err := SyncItemGet(db, p)
	if err != nil {
		return nil, e.Wrap(err, e.Code060A, "01")
	}

	if len(siList) == 0 {
		return nil, e.Wrap(fmt.Errorf("no items found"), e.Code060A, "02")
	}

	return siList[0], nil
}
