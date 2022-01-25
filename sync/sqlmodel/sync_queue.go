package sqlmodel

import (
	"fmt"
	"strings"

	"github.com/Skyrin/go-lib/e"
	"github.com/Skyrin/go-lib/sql"
	"github.com/Skyrin/go-lib/sync/model"
	"github.com/lib/pq"
)

const (
	SyncQueueTableName     = "sync_queue"
	SyncQueueDefaultSortBy = "sync_queue_id"

	ECode060301 = e.Code0603 + "01"
	ECode060302 = e.Code0603 + "02"
	ECode060303 = e.Code0603 + "03"
	ECode060304 = e.Code0603 + "04"
	ECode060305 = e.Code0603 + "05"
	ECode060306 = e.Code0603 + "06"
	ECode060307 = e.Code0603 + "07"
	ECode060308 = e.Code0603 + "08"
	ECode060309 = e.Code0603 + "09"
	ECode06030A = e.Code0603 + "0A"
	ECode06030B = e.Code0603 + "0B"
	ECode06030C = e.Code0603 + "0C"
	ECode06030D = e.Code0603 + "0D"
	ECode06030E = e.Code0603 + "0E"
	ECode06030F = e.Code0603 + "0F"
	ECode06030G = e.Code0603 + "0G"
	ECode06030H = e.Code0603 + "0H"
)

// SyncQueueGetParam model
type SyncQueueGetParam struct {
	Limit            *uint64
	Offset           *uint64
	ID               *int
	ItemID           *int
	Status           *[]string
	Service          *[]string
	ItemType         *string
	ForDelete        *bool
	FlagCount        bool
	FlagForUpdate    bool
	OrderByID        string
	OrderByService   string
	OrderByUpdatedOn string
	DataHandler      func(*model.SyncQueue) error
}

// Upsert performs the DB operation to upsert a record in the sync_queue table
func Upsert(db *sql.Connection, input *model.SyncQueue) (id int, err error) {
	ib := db.Insert(SyncQueueTableName).
		Columns(`sync_queue_status, sync_queue_item,
			sync_queue_retries, sync_queue_service, sync_queue_delete, sync_queue_item_type,
			sync_queue_item_id, sync_queue_item_hash, 
			created_on, updated_on`).
		Values(input.Status, input.Item,
			input.Retries, input.Service, input.ForDelete, input.ItemType,
			input.ItemID, input.ItemHash,
			"now()", "now()").
		Suffix(`ON CONFLICT (sync_queue_service, sync_queue_item_type, sync_queue_item_id) 
			DO UPDATE
			SET			
			sync_queue_item=excluded.sync_queue_item,
			sync_queue_item_hash=excluded.sync_queue_item_hash,
			sync_queue_status=excluded.sync_queue_status,
			sync_queue_delete=excluded.sync_queue_delete,
			updated_on=now()
			RETURNING sync_queue_id`)

	id, err = db.ExecInsertReturningID(ib)
	if err != nil {
		return 0, e.W(err, ECode060301)
	}

	return id, nil
}

// SyncQueueSetStatus updates the status
func SyncQueueSetStatus(db *sql.Connection, id int, status string) (err error) {
	ub := db.Update(SyncQueueTableName).
		Where("sync_queue_id=?", id).
		Set("sync_queue_status", status).
		Set("updated_on", "now()")

	if status == model.SyncQueueStatusComplete {
		ub = ub.Set("sync_queue_retries", 0).
			Set("sync_queue_error", "")
	}

	if err := db.ExecUpdate(ub); err != nil {
		return e.W(err, ECode060302)
	}

	return nil
}

// SyncQueueSetHash sets the hash for the item to be synced
func SyncQueueSetHash(db *sql.Connection, id int, hash string) (err error) {
	ub := db.Update(SyncQueueTableName).
		Where("sync_queue_id=?", id).
		Set("sync_queue_item_hash", hash).
		Set("updated_on", "now()")

	if err := db.ExecUpdate(ub); err != nil {
		return e.W(err, ECode060303)
	}

	return nil
}

// SyncQueueSetItemHashAndStatus sets the  item, hash for the item, and status to be synced
func SyncQueueSetItemHashAndStatus(db *sql.Connection, id int,
	hash, status string, item *[]byte) (err error) {
	ub := db.Update(SyncQueueTableName).
		Where("sync_queue_id=?", id).
		Set("sync_queue_item_hash", hash).
		Set("sync_queue_item", item).
		Set("sync_queue_status", status).
		Set("updated_on", "now()")

	if err := db.ExecUpdate(ub); err != nil {
		return e.W(err, ECode060304)
	}

	return nil
}

// SyncQueueSetError set error for sync
func SyncQueueSetError(db *sql.Connection, id int, msg string) (err error) {
	ub := db.Update(SyncQueueTableName).
		Where("sync_queue_id=?", id).
		Set("sync_queue_status", model.SyncQueueStatusFailed).
		Set("sync_queue_error", msg).
		Set("sync_queue_retries", db.Expr("sync_queue_retries + ?", 1)).
		Set("updated_on", "now()")

	if err := db.ExecUpdate(ub); err != nil {
		return e.W(err, ECode060305)
	}

	return nil
}

// SyncQueueSetDelete updates the delete flag
func SyncQueueSetDelete(db *sql.Connection, id int, delete bool) (err error) {
	ub := db.Update(SyncQueueTableName).
		Where("sync_queue_id=?", id).
		Set("sync_queue_delete", delete).
		Set("updated_on", "now()")

	if delete {
		ub = ub.Set("sync_queue_status", model.SyncQueueStatusPending)
	}

	if err := db.ExecUpdate(ub); err != nil {
		return e.W(err, ECode060306)
	}

	return nil
}

// SyncQueueSetDeleteByServiceItemTypeAndItemID updates the delete flag
func SyncQueueSetDeleteByServiceItemTypeAndItemID(db *sql.Connection, itemID int, itemType, service string,
	delete bool) (err error) {
	ub := db.Update(SyncQueueTableName).
		Where("sync_queue_item_id=?", itemID).
		Set("sync_queue_delete", delete).
		Set("sync_queue_item_type", itemType).
		Set("sync_queue_service", service).
		Set("updated_on", "now()")

	if delete {
		ub = ub.Set("sync_queue_status", model.SyncQueueStatusPending)
	}

	if err := db.ExecUpdate(ub); err != nil {
		return e.W(err, ECode060307)
	}

	return nil
}

// SyncQueueGet performs select
func SyncQueueGet(db *sql.Connection,
	p *SyncQueueGetParam) (sqList []*model.SyncQueue, count int, err error) {
	fields := `sync_queue_id, sync_queue_item,
		sync_queue_status, sync_queue_retries, sync_queue_service, sync_queue_delete,
		sync_queue_item_type, sync_queue_item_id, sync_queue_item_hash, 
		created_on, updated_on`

	sb := db.Select("{fields}").
		From(SyncQueueTableName)

	if p.Limit != nil && *p.Limit > 0 {
		sb = sb.Limit(*p.Limit)
	}

	if p.FlagForUpdate {
		sb = sb.Suffix("FOR UPDATE")
	}

	if p.ID != nil && *p.ID >= 0 {
		sb = sb.Where("sync_queue_id=?", *p.ID)
	}

	if p.ItemID != nil && *p.ItemID >= 0 {
		sb = sb.Where("sync_queue_item_id=?", *p.ItemID)
	}

	if p.Status != nil && len(*p.Status) > 0 {
		sb = sb.Where("sync_queue_status = ANY(?)", pq.Array(*p.Status))
	}

	if p.Service != nil && len(*p.Service) > 0 {
		sb = sb.Where("sync_queue_service = ANY(?)", pq.Array(*p.Service))
	}

	if p.ItemType != nil && len(*p.ItemType) > 0 {
		sb = sb.Where("sync_queue_item_type=?", *p.ItemType)
	}

	if p.ForDelete != nil {
		sb = sb.Where("sync_queue_delete=?", *p.ForDelete)
	}

	stmt, bindList, err := sb.ToSql()
	if err != nil {
		return nil, 0, e.W(err, ECode060308)
	}

	if p.FlagCount {
		row := db.QueryRow(strings.Replace(stmt, "{fields}", "count(*)", 1), bindList...)
		if err := row.Scan(&count); err != nil {
			return nil, 0, e.W(err, ECode060309,
				fmt.Sprintf("SyncQueueGet.2 | stmt: %s, bindList: %+v",
					stmt, bindList))
		}
	}

	if p.Offset != nil {
		sb = sb.Offset(uint64(*p.Offset))
	}

	orderByDefault := true

	if p.OrderByID != "" {
		sb = sb.OrderBy(fmt.Sprintf("sync_queue_id %s", p.OrderByID))
		orderByDefault = false
	}

	if p.OrderByService != "" {
		sb = sb.OrderBy(fmt.Sprintf("sync_queue_service %s", p.OrderByService))
		orderByDefault = false
	}

	if p.OrderByUpdatedOn != "" {
		sb = sb.OrderBy(fmt.Sprintf("updated_on %s", p.OrderByUpdatedOn))
		orderByDefault = false
	}

	if orderByDefault {
		sb = sb.OrderBy(fmt.Sprintf("%s %s", SyncQueueDefaultSortBy, "asc"))
	}

	stmt, bindList, err = sb.ToSql()
	stmt = strings.Replace(stmt, "{fields}", fields, 1)
	rows, err := db.Query(stmt, bindList...)
	if err != nil {
		return nil, 0, e.W(err, ECode06030A)
	}
	defer rows.Close()

	for rows.Next() {
		sq := &model.SyncQueue{}
		if err := rows.Scan(&sq.ID, &sq.Item,
			&sq.Status, &sq.Retries, &sq.Service, &sq.ForDelete,
			&sq.ItemType, &sq.ItemID, &sq.ItemHash,
			&sq.CreatedOn, &sq.UpdatedOn); err != nil {
			return nil, 0, e.W(err, ECode06030B)
		}

		if p.DataHandler != nil {
			if err := p.DataHandler(sq); err != nil {
				return nil, 0, e.W(err, ECode06030C)
			}
		} else {
			sqList = append(sqList, sq)
		}
	}

	return sqList, count, nil
}

// SyncQueueGetByStatus returns the items with the specified status for the specified services
func SyncQueueGetByStatusService(db *sql.Connection, status, service []string,
	limit *uint64) (sqList []*model.SyncQueue, count int, err error) {
	p := &SyncQueueGetParam{
		Status:  &status,
		Limit:   limit,
		Service: &service,
	}

	return SyncQueueGet(db, p)
}

// SyncQueueGetByItemIDTypeAndService searches by the item id
func SyncQueueGetByItemIDTypeAndService(db *sql.Connection, itemID int,
	itemType, service string) (sq *model.SyncQueue, err error) {
	limit := uint64(1)
	serviceList := []string{service}
	p := &SyncQueueGetParam{
		Limit:    &limit,
		ItemID:   &itemID,
		Service:  &serviceList,
		ItemType: &itemType,
	}

	sqList, _, err := SyncQueueGet(db, p)
	if err != nil {
		return nil, e.W(err, ECode06030D)
	}

	if len(sqList) == 0 {
		return nil, e.N(ECode06030E, "no items found")
	}

	return sqList[0], nil
}

// SyncQueueGetItemIDsByServiceAndItemType Get list of all items IDs for a specified service
func SyncQueueGetItemIDsByServiceAndItemType(db *sql.Connection, limit, offset int,
	status []string, itemType, service string) (idList []int, count int, err error) {
	if len(service) == 0 {
		return nil, 0, e.N(ECode06030F, "service cannot be blank")
	}

	serviceList := []string{service}
	fields := `sync_queue_item_id`

	sb := db.Select("{fields}").
		From(SyncQueueTableName).
		OrderBy("sync_queue_item_id asc").
		Limit(uint64(limit)).
		Offset(uint64(offset)).
		Where("sync_queue_service = ANY(?)", pq.Array(serviceList)).
		Where("sync_queue_item_type=?", itemType)

	if len(status) > 0 {
		sb = sb.Where("sync_queue_status = ANY(?)", pq.Array(status))
	}

	stmt, bindList, err := sb.ToSql()
	stmt = strings.Replace(stmt, "{fields}", fields, 1)
	rows, err := db.Query(stmt, bindList...)
	if err != nil {
		return nil, 0, e.W(err, ECode06030G)
	}
	defer rows.Close()

	for rows.Next() {
		var id int
		if err := rows.Scan(&id); err != nil {
			return nil, 0, e.W(err, ECode06030H)
		}

		idList = append(idList, id)
	}

	return idList, len(idList), nil
}
