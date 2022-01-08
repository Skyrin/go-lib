package sqlmodel

import (
	"crypto/sha512"
	"fmt"
	"strings"

	"github.com/Skyrin/go-lib/arc/model"
	"github.com/Skyrin/go-lib/e"
	"github.com/Skyrin/go-lib/sql"
)

const (
	DataTableName = "arc_data"
)

// DataGetParam model
type DataGetParam struct {
	Limit     uint64
	Offset    uint64
	AppCode   *model.AppCode
	AppCoreID *uint
	Type      *model.DataType
	ObjectID  *uint
	Status    *model.DataStatus
	FlagCount bool
	OrderBy   string
}

// DataSetStatus set the status for the specified data record
func DataSetStatus(db *sql.Connection, s model.DataStatus, d *model.Data) (err error) {
	ub := db.Update(DataTableName).
		Set("arc_data_status=?", s).
		Set("updated_on=?", "now()").
		Where("arc_app_code", d.AppCode).
		Where("arc_app_core_id", d.AppCoreID).
		Where("arc_data_type", d.Type).
		Where("arc_data_object_id", d.ObjectID)

	err = db.ExecUpdate(ub)
	if err != nil {
		return e.Wrap(err, e.Code0414, "02")
	}

	return nil
}

// DataUpsert upserts the record and only updates an existing record if the hash
// has changed or if the deleted flag has changed. When updating, it will calculate
// the hash and set the new object, hash and/or deleted flag. Also, it sets the
// status to pending in this scenario
func DataUpsert(db *sql.Connection, d *model.Data) (err error) {
	// Calculate the hash
	d.Hash = sha512.Sum512_256(d.Object)

	ib := db.Insert(DataTableName).Columns(`
			arc_app_code,arc_app_core_id,arc_data_type,arc_data_object_id,
			arc_data_status,arc_data_object,arc_data_hash,arc_data_deleted,
			created_on,updated_on`).
		Values(d.AppCode, d.AppCoreID, d.Type, d.ObjectID,
			model.DataStatusPending, d.Object, d.Hash[:], d.Deleted,
			"now()", "now()").
		Suffix(`ON CONFLICT
			(arc_app_code,arc_app_core_id,arc_data_type,arc_data_object_id)
		DO UPDATE
		SET arc_data_status=excluded.arc_data_status,
			arc_data_object=excluded.arc_data_object,
			arc_data_hash=excluded.arc_data_hash,
			arc_data_deleted=excluded.arc_data_deleted,
			updated_on=now()
		WHERE arc_data.arc_data_hash != excluded.arc_data_hash
			OR arc_data.arc_data_deleted != excluded.arc_data_deleted`)

	err = db.ExecInsert(ib)
	if err != nil {
		return e.Wrap(err, e.Code0411, "01")
	}

	return nil
}

// DataGet performs select
func DataGet(db *sql.Connection,
	p *DataGetParam) (dList []*model.Data, count int, err error) {

	fields := `arc_app_code,arc_app_core_id,arc_data_type,arc_data_object_id
	arc_data_status,arc_data_object,arc_data_hash,arc_data_deleted,
	created_on,updated_on`

	sb := db.Select("{fields}").
		From(DataTableName)

	if p.Limit > 0 {
		sb = sb.Limit(p.Limit)
	}

	if p.AppCode != nil {
		sb = sb.Where("arc_app_code=?", *p.AppCode)
	}

	if p.AppCoreID != nil {
		sb = sb.Where("arc_app_core_id=?", *p.AppCoreID)
	}

	if p.Type != nil {
		sb = sb.Where("arc_data_type=?", *p.Type)
	}

	if p.ObjectID != nil {
		sb = sb.Where("arc_data_object_id=?", *p.ObjectID)
	}

	if p.Status != nil {
		sb = sb.Where("arc_data_status=?", *p.Status)
	}

	stmt, bindList, err := sb.ToSql()
	if err != nil {
		return nil, 0, e.Wrap(err, e.Code0412, "01")
	}

	if p.FlagCount {
		row := db.QueryRow(strings.Replace(stmt, "{fields}", "count(*)", 1), bindList...)
		if err := row.Scan(&count); err != nil {
			return nil, 0, e.Wrap(err, e.Code0412, "02",
				fmt.Sprintf("stmt: %s, bindList: %+v", stmt, bindList))
		}
	}

	if p.Offset > 0 {
		sb = sb.Offset(uint64(p.Offset))
	}

	if p.OrderBy != "" {
		sb = sb.OrderBy(fmt.Sprintf(
			"arc_app_code %s, arc_app_core_id %s, arc_data_type %s, arc_data_object_id %s",
			p.OrderBy, p.OrderBy, p.OrderBy, p.OrderBy))
	}

	stmt, bindList, err = sb.ToSql()
	stmt = strings.Replace(stmt, "{fields}", fields, 1)

	rows, err := db.Query(stmt, bindList...)
	if err != nil {
		return nil, 0, e.Wrap(err, e.Code0412, "03")
	}
	defer rows.Close()

	for rows.Next() {
		d := &model.Data{}
		if err := rows.Scan(&d.AppCode, &d.AppCoreID, &d.Type, &d.ObjectID,
			&d.Object, &d.Hash, &d.Status, &d.Deleted,
			&d.CreatedOn, &d.UpdatedOn); err != nil {
			return nil, 0, e.Wrap(err, e.Code0412, "04")
		}

		dList = append(dList, d)
	}

	return dList, count, nil
}

// DataGetByObjectID returns record associated with the object id, must also include
// the app code, app core id and data type to be unique
func DataGetByObjectID(db *sql.Connection, appCode model.AppCode,
	appCoreID uint, t model.DataType, objectID uint) (d *model.Data, err error) {
	dList, _, err := DataGet(db, &DataGetParam{
		Limit:     1,
		AppCode:   &appCode,
		AppCoreID: &appCoreID,
		Type:      &t,
		ObjectID:  &objectID,
	})

	if err != nil {
		return nil, e.Wrap(err, e.Code0413, "01")
	}

	if len(dList) != 1 {
		return nil, e.New(e.Code0413, "02", e.MsgDataDoesNotExist)
	}

	return dList[0], nil
}
