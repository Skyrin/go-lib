package sqlmodel

import (
	"context"
	"crypto/sha512"
	"fmt"
	"strings"

	"github.com/Skyrin/go-lib/arcpgx/model"
	"github.com/Skyrin/go-lib/e"
	sql "github.com/Skyrin/go-lib/sqlpgx"
)

const (
	DataTableName = "arc_data"

	ECode040F01 = e.Code040F + "01"
	ECode040F02 = e.Code040F + "02"
	ECode040F03 = e.Code040F + "03"
	ECode040F04 = e.Code040F + "04"
	ECode040F05 = e.Code040F + "05"
	ECode040F06 = e.Code040F + "06"
	ECode040F07 = e.Code040F + "07"
	ECode040F08 = e.Code040F + "08"
	ECode040F09 = e.Code040F + "09"
	ECode040F0A = e.Code040F + "0A"
	ECode040F0B = e.Code040F + "0B"
)

// DataGetParam model
type DataGetParam struct {
	Limit           uint64
	Offset          uint64
	DeploymentID    *int
	AppCode         *model.AppCode
	AppCoreID       *uint
	Type            *model.DataType
	ObjectID        *uint
	Status          *model.DataStatus
	FlagCount       bool
	OrderBy         string
	OrderByTypeList []model.DataType
	Handle          func(*model.Data) error
}

// DataSetStatus set the status for the specified data record
func DataSetStatus(ctx context.Context, db *sql.Connection, s model.DataStatus, d *model.Data) (err error) {
	ub := db.Update(DataTableName).
		Set("arc_data_status", s).
		Set("updated_on", "now()").
		Where("arc_deployment_id=?", d.DeploymentID).
		Where("arc_app_code=?", d.AppCode).
		Where("arc_app_core_id=?", d.AppCoreID).
		Where("arc_data_type=?", d.Type).
		Where("arc_data_object_id=?", d.ObjectID)

	err = db.ExecUpdate(ctx, ub)
	if err != nil {
		return e.W(err, ECode040F01)
	}

	return nil
}

// DataUpsert upserts the record and only updates an existing record if the hash
// has changed or if the deleted flag has changed. When updating, it will calculate
// the hash and set the new object, hash and/or deleted flag. Also, it sets the
// status to pending in this scenario
func DataUpsert(ctx context.Context, db *sql.Connection, deploymentID int, d *model.Data) (err error) {
	// Calculate the hash
	hash := sha512.Sum512_256(d.Object)
	d.Hash = hash[:]

	ib := db.Insert(DataTableName).Columns(`
			arc_deployment_id,arc_app_code,arc_app_core_id,arc_data_type,arc_data_object_id,
			arc_data_status,arc_data_object,arc_data_hash,arc_data_deleted,
			created_on,updated_on`).
		Values(deploymentID, d.AppCode, d.AppCoreID, d.Type, d.ObjectID,
			model.DataStatusPending, d.Object, d.Hash, d.Deleted,
			"now()", "now()").
		Suffix(`ON CONFLICT ON CONSTRAINT arc_data__pkey
--			(arc_deployment_id,arc_app_code,arc_app_core_id,arc_data_type,arc_data_object_id)
		DO UPDATE
		SET arc_data_status=excluded.arc_data_status,
			arc_data_object=excluded.arc_data_object,
			arc_data_hash=excluded.arc_data_hash,
			arc_data_deleted=excluded.arc_data_deleted,
			updated_on=now()
		WHERE arc_data.arc_data_hash != excluded.arc_data_hash
			OR arc_data.arc_data_deleted != excluded.arc_data_deleted`)

	err = db.ExecInsert(ctx, ib)
	if err != nil {
		return e.W(err, ECode040F02)
	}

	return nil
}

// DataGet performs select
func DataGet(ctx context.Context, db *sql.Connection,
	p *DataGetParam) (dList []*model.Data, count int, err error) {

	fields := `arc_deployment_id,arc_app_code,arc_app_core_id,arc_data_type,arc_data_object_id,
	arc_data_object,arc_data_hash,arc_data_deleted,arc_data_status,
	created_on,updated_on`

	sb := db.Select("{fields}").
		From(DataTableName)

	if p.Limit > 0 {
		sb = sb.Limit(p.Limit)
	}

	if p.DeploymentID != nil {
		sb = sb.Where("arc_deployment_id=?", *p.DeploymentID)
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
		return nil, 0, e.W(err, ECode040F03)
	}

	if p.FlagCount {
		row := db.QueryRow(ctx, strings.Replace(stmt, "{fields}", "count(*)", 1), bindList...)
		if err := row.Scan(&count); err != nil {
			return nil, 0, e.W(err, ECode040F04,
				fmt.Sprintf("stmt: %s, bindList: %+v", stmt, bindList))
		}
	}

	if p.Offset > 0 {
		sb = sb.Offset(uint64(p.Offset))
	}

	if p.OrderBy != "" {
		sb = sb.OrderBy(fmt.Sprintf(
			"arc_deployment_id %s, arc_app_code %s, arc_app_core_id %s, arc_data_type %s, arc_data_object_id %s",
			p.OrderBy, p.OrderBy, p.OrderBy, p.OrderBy, p.OrderBy))
	}

	if p.OrderByTypeList != nil {
		s := strings.Builder{}
		_, _ = s.WriteString(`CASE "arc_data_type" `)
		for idx, o := range p.OrderByTypeList {
			_, _ = s.WriteString(fmt.Sprintf(` WHEN '%s' THEN %d `, o, idx))
		}
		_, _ = s.WriteString(`END ASC`)
		sb = sb.OrderBy(s.String())
	}

	stmt, bindList, err = sb.ToSql()
	stmt = strings.Replace(stmt, "{fields}", fields, 1)

	rows, err := db.Query(ctx, stmt, bindList...)
	if err != nil {
		return nil, 0, e.W(err, ECode040F05)
	}
	defer rows.Close()

	for rows.Next() {
		d := &model.Data{}
		if err := rows.Scan(&d.DeploymentID, &d.AppCode, &d.AppCoreID, &d.Type, &d.ObjectID,
			&d.Object, &d.Hash, &d.Deleted, &d.Status,
			&d.CreatedOn, &d.UpdatedOn); err != nil {
			return nil, 0, e.W(err, ECode040F06)
		}

		if p.Handle != nil {
			if err := p.Handle(d); err != nil {
				return nil, 0, e.W(err, ECode040F07)
			}
		} else {
			dList = append(dList, d)
		}
	}

	return dList, count, nil
}

// DataGetByObjectID returns record associated with the object id, must also include
// the app code, app core id and data type to be unique
func DataGetByObjectID(ctx context.Context, db *sql.Connection, deploymentID int, appCode model.AppCode,
	appCoreID uint, t model.DataType, objectID uint) (d *model.Data, err error) {
	dList, _, err := DataGet(ctx, db, &DataGetParam{
		Limit:        1,
		DeploymentID: &deploymentID,
		AppCode:      &appCode,
		AppCoreID:    &appCoreID,
		Type:         &t,
		ObjectID:     &objectID,
	})

	if err != nil {
		return nil, e.W(err, ECode040F08)
	}

	if len(dList) != 1 {
		return nil, e.N(ECode040F09, e.MsgDataDoesNotExist)
	}

	return dList[0], nil
}

// DataSetStatusProcessing set all records that are pending to processing
func DataSetStatusProcessing(ctx context.Context, db *sql.Connection) (err error) {
	ub := db.Update(DataTableName).
		Set("arc_data_status", string(model.DataStatusProcessing)).
		Set("updated_on", "now()").
		Where("arc_data_status=?", string(model.DataStatusPending))

	err = db.ExecUpdate(ctx, ub)
	if err != nil {
		return e.W(err, ECode040F0A)
	}

	return nil
}

// DataSetStatusProcessed set all records that are processing to processed
func DataSetStatusProcessed(ctx context.Context, db *sql.Connection) (err error) {
	ub := db.Update(DataTableName).
		Set("arc_data_status", model.DataStatusProcessed).
		Set("updated_on", "now()").
		Where("arc_data_status=?", model.DataStatusProcessing)

	err = db.ExecUpdate(ctx, ub)
	if err != nil {
		return e.W(err, ECode040F0B)
	}

	return nil
}
