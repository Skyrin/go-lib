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
	ECode070808 = e.Code0708 + "08"
	ECode070809 = e.Code0708 + "09"
	ECode07080A = e.Code0708 + "0A"
	ECode07080B = e.Code0708 + "0B"
	ECode07080C = e.Code0708 + "0C"
	ECode07080D = e.Code0708 + "0D"
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

// DataBulkInsert optimized way to upsert records
type DataBulkInsert struct {
	bi *sql.BulkInsert
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
	if err != nil {
		return nil, 0, e.W(err, ECode07080D)
	}

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

// DataGetByPubIDDataTypeAndDataID fetch the specific record
func DataGetByPubIDDataTypeAndDataID(db *sql.Connection,
	pubID int, dataType, dataID string) (d *model.Data, err error) {
	p := &DataGetParam{
		PubID: &pubID,
		Type:  &dataType,
		ID:    &dataID,
	}

	dList, _, err := DataGet(db, p)
	if err != nil {
		return nil, e.W(err, ECode07080B)
	}

	if len(dList) != 1 {
		return nil, e.N(ECode07080C, "not found")
	}

	return dList[0], nil
}

// NewDataBulkInsert initializes and returns a new bulk insert for creating/updating pub data
// records. If updating, it will increment the version and update the deleted/json values
// Note: whatever calls this must call Flush and Close when done
func NewDataBulkInsert(db *sql.Connection) (sdbc *DataBulkInsert) {
	sdbc = &DataBulkInsert{}
	sdbc.bi, _ = sql.NewBulkInsert(db, DataTableName, DataColumns, DataUpsertOnConflict)

	return sdbc
}

// Add adds the item to the bulk insert. If it saves to the database, it will return the
// number of rows added
func (b *DataBulkInsert) Add(d *model.Data) (rowsAdded int, err error) {
	values, err := d.InsertValues()
	if err != nil {
		return 0, e.W(err, ECode070808)
	}

	rowsAdded, err = b.bi.Add(values...)
	if err != nil {
		return 0, e.W(err, ECode070809,
			fmt.Sprintf("pubId: %d, data type: %s, data id: %s, deleted: %v, version: %d",
				d.PubID, d.Type, d.ID, d.Deleted, d.Version))
	}

	return rowsAdded, nil
}

// Flush saves any pending records
func (b *DataBulkInsert) Flush() (err error) {
	if err = b.bi.Flush(); err != nil {
		return e.W(err, ECode07080A)
	}

	return nil
}

// Close closes all open statements
func (b *DataBulkInsert) Close() (errList []error) {
	return b.bi.Close()
}

// GetCount returns the number of rows added since the last flush call
func (b *DataBulkInsert) GetCount() (count int) {
	return b.bi.GetCount()
}

// GetTotal returns the total number of rows added
func (b *DataBulkInsert) GetTotal() (count int) {
	return b.bi.GetTotal()
}
