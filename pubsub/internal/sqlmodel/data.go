package sqlmodel

import (
	"fmt"
	"strings"

	"github.com/Skyrin/go-lib/e"
	"github.com/Skyrin/go-lib/pubsub/model"
	"github.com/Skyrin/go-lib/sql"
)

const (
	DataTableName = "skyrin_dps_data"
	DataColumns   = `dps_pub_id, dps_data_type, dps_data_id, 
		dps_data_deleted, dps_data_version, dps_data_json,
		created_on, updated_on`
	DataUpsertOnConflict = `ON CONFLICT (dps_pub_id, dps_data_type, dps_data_id) 
		DO UPDATE
		SET
		dps_data_deleted=EXCLUDED.dps_data_deleted,
		dps_data_version=` + DataTableName + `.dps_data_version+1,
		dps_data_json=COALESCE(EXCLUDED.dps_data_json,` + DataTableName + `.dps_data_json),
		updated_on=now()`
	DataUpsertOnConflictReturning = DataUpsertOnConflict + " RETURNING dps_data_version"

	ECode070801 = e.Code0708 + "01"
	ECode070802 = e.Code0708 + "02"
	ECode070803 = e.Code0708 + "03"
	ECode070804 = e.Code0708 + "04"
	ECode070805 = e.Code0708 + "05"
	ECode070806 = e.Code0708 + "06"
	ECode070807 = e.Code0708 + "07"
)

// DataGetParam model
type DataGetParam struct {
	Limit         *uint64
	Offset        *uint64
	NoLimit       bool
	PubID         *int
	Type          *string
	ID            *string
	Search        *string
	FlagCount     bool
	FlagForUpdate bool
	OrderByID     string
	DataHandler   func(*model.Data) error
}

// DataUpsert inserts a record, returning the version
func DataUpsert(db *sql.Connection, input *model.Data) (version int, err error) {
	values, err := input.InsertValues()
	if err != nil {
		return 0, e.W(err, ECode070801)
	}

	ib := db.Insert(DataTableName).
		Columns(DataColumns).
		Values(values...).
		Suffix(DataUpsertOnConflictReturning)
	version, err = db.ExecInsertReturningID(ib)
	if err != nil {
		return 0, e.W(err, ECode070802)
	}

	return version, nil
}

// DataGet performs select
func DataGet(db *sql.Connection,
	p *DataGetParam) (sList []*model.Data, count int, err error) {
	fields := DataColumns

	sb := db.Select("{fields}").
		From(DataTableName)

	if p.FlagForUpdate {
		sb = sb.Suffix("FOR UPDATE")
	}

	if p.PubID != nil {
		sb = sb.Where("dps_data_id=?", *p.ID)
	}

	if p.Type != nil {
		sb = sb.Where("dps_data_type=?", *p.Type)
	}

	if p.ID != nil {
		sb = sb.Where("dps_data_id=?", *p.ID)
	}

	if p.Search != nil {
		sb = sb.Where(db.Expr("(dps_data_type like ? OR dps_data_id like ?)", *p.Search, *p.Search))
	}

	stmt, bindList, err := sb.ToSql()
	if err != nil {
		return nil, 0, e.W(err, ECode070803)
	}

	if p.FlagCount {
		row := db.QueryRow(strings.Replace(stmt, "{fields}", "count(*)", 1), bindList...)
		if err := row.Scan(&count); err != nil {
			return nil, 0, e.W(err, ECode070804,
				fmt.Sprintf("bindList: %+v", bindList))
		}
	}

	if p.OrderByID != "" {
		sb = sb.OrderBy(fmt.Sprintf("dps_pub_id, dps_data_type, dps_data_id %s", p.OrderByID))
	}

	if p.Limit != nil && *p.Limit > 0 {
		sb = sb.Limit(*p.Limit)
	}

	if p.Offset != nil {
		sb = sb.Offset(uint64(*p.Offset))
	}

	stmt, bindList, err = sb.ToSql()
	stmt = strings.Replace(stmt, "{fields}", fields, 1)
	rows, err := db.Query(stmt, bindList...)
	if err != nil {
		return nil, 0, e.W(err, ECode070805)
	}
	defer rows.Close()

	for rows.Next() {
		sq := &model.Data{}
		if err := rows.Scan(sq.ScanPointers()...); err != nil {
			return nil, 0, e.W(err, ECode070806)
		}

		if p.DataHandler != nil {
			if err := p.DataHandler(sq); err != nil {
				return nil, 0, e.W(err, ECode070807)
			}
		} else {
			sList = append(sList, sq)
		}
	}

	return sList, count, nil
}
