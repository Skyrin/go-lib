package sqlmodel

import (
	"fmt"
	"strings"

	"github.com/Skyrin/go-lib/e"
	"github.com/Skyrin/go-lib/pubsub/model"
	"github.com/Skyrin/go-lib/sql"
)

const (
	SubDataTableName = "skyrin_dps_sub_data"
	SubDataColumns   = `dps_sub_id, dps_pub_id, dps_data_type, dps_data_id, 
		dps_sub_data_deleted, dps_sub_data_version, dps_sub_data_status,
		dps_sub_data_hash, dps_sub_data_json,
		dps_sub_data_retries, dps_sub_data_message,
		created_on, updated_on`
	SubDataColumnsWithID    = `dps_sub_data_id,` + SubDataColumns
	SubDataUpsertOnConflict = `ON CONFLICT (dps_sub_id, dps_pub_id, dps_data_type, dps_data_id)
		DO UPDATE
		SET
		dps_sub_data_deleted=EXCLUDED.dps_sub_data_deleted,
		dps_sub_data_version=EXCLUDED.dps_sub_data_version,
		dps_sub_data_status=EXCLUDED.dps_sub_data_status,
		dps_sub_data_hash=CASE
			WHEN EXCLUDED.dps_sub_data_hash <> '' THEN EXCLUDED.dps_sub_data_hash
			ELSE ` + SubDataTableName + `.dps_sub_data_hash
		END,
		dps_sub_data_json=COALESCE(EXCLUDED.dps_sub_data_json,` + SubDataTableName + `.dps_sub_data_json),
		dps_sub_data_retries=COALESCE(EXCLUDED.dps_sub_data_retries,` + SubDataTableName + `.dps_sub_data_retries),
		dps_sub_data_message=EXCLUDED.dps_sub_data_message,
		updated_on=NOW()
		WHERE ` + SubDataTableName + `.dps_sub_data_version<EXCLUDED.dps_sub_data_version
			OR (`+
			SubDataTableName + `.dps_sub_data_version=EXCLUDED.dps_sub_data_version AND ` +
			SubDataTableName + `.dps_sub_data_status='pending')`

	stmtSubDataCreateFromData = `INSERT INTO ` + SubDataTableName + ` (` + SubDataColumns + `)
		SELECT
			$1, d.dps_pub_id, d.dps_data_type, d.dps_data_id, 
			d.dps_data_deleted, d.dps_data_version, 'pending', 
			'', NULL, 
			0, '', 
			NOW(), NOW()
		FROM ` + DataTableName + ` AS d
		INNER JOIN ` + PubSubMapTableName + ` AS psm ON d.dps_pub_id=psm.dps_pub_id AND psm.dps_sub_id=$1
		LEFT JOIN ` + SubDataTableName + ` AS sd 
			ON d.dps_pub_id=sd.dps_pub_id AND d.dps_data_type =sd.dps_data_type AND d.dps_data_id =sd.dps_data_id
		WHERE sd.dps_sub_id IS NULL
		ON CONFLICT DO NOTHING`

	stmtSubDataUpdateFromData = `UPDATE ` + SubDataTableName + ` AS sd
		SET dps_sub_data_deleted=d.dps_data_deleted,
			dps_sub_data_version=d.dps_data_version,
			dps_sub_data_status='pending',
			dps_sub_data_retries=0,
			updated_on=NOW()
		FROM ` + DataTableName + ` AS d
		WHERE sd.dps_sub_id=$1
			AND d.dps_pub_id=sd.dps_pub_id AND d.dps_data_type=sd.dps_data_type AND d.dps_data_id=sd.dps_data_id
			AND d.dps_data_version > sd.dps_sub_data_version`

	stmtSubDataGetProcessable = `SELECT 
		sd.dps_sub_data_id, sd.dps_pub_id, sd.dps_data_type, sd.dps_data_id, 
		sd.dps_sub_data_deleted, sd.dps_sub_data_version,
		sd.dps_sub_data_status, sd.dps_sub_data_hash, sd.dps_sub_data_retries
		FROM ` + SubDataTableName + ` AS sd
		WHERE sd.dps_sub_data_id>$1 AND sd.dps_sub_id=$2 AND sd.dps_sub_data_status='pending'
		ORDER BY sd.dps_sub_data_id ASC
		LIMIT $3
		FOR UPDATE SKIP LOCKED`

	// Error constants
	ECode070901 = e.Code0709 + "01"
	ECode070902 = e.Code0709 + "02"
	ECode070903 = e.Code0709 + "03"
	ECode070904 = e.Code0709 + "04"
	ECode070905 = e.Code0709 + "05"
	ECode070906 = e.Code0709 + "06"
	ECode070907 = e.Code0709 + "07"
	ECode070908 = e.Code0709 + "08"
	ECode070909 = e.Code0709 + "09"
	ECode07090A = e.Code0709 + "0A"
	ECode07090B = e.Code0709 + "0B"
	ECode07090C = e.Code0709 + "0C"
	ECode07090D = e.Code0709 + "0D"
	ECode07090E = e.Code0709 + "0E"
)

// SubDataGetParam model
type SubDataGetParam struct {
	Limit          *uint64
	Offset         *uint64
	NoLimit        bool
	SubID          *int
	PubID          *int
	Type           *string
	ID             *string
	VersionLess    *int
	FlagCount      bool
	FlagForUpdate  bool
	OrderByID      string
	SubDataHandler func(*model.SubData) error
}

// SubDataUpsert inserts a record
func SubDataUpsert(db *sql.Connection, sd *model.SubData) (err error) {
	values, err := sd.InsertValues()
	if err != nil {
		return e.W(err, ECode070901)
	}
	ib := db.Insert(SubDataTableName).
		Columns(SubDataColumns).
		Values(values...).
		Suffix(SubDataUpsertOnConflict)

	if err = db.ExecInsert(ib); err != nil {
		return e.W(err, ECode070902)
	}

	return nil
}

// SubDataGet performs select
func SubDataGet(db *sql.Connection,
	p *SubDataGetParam) (sList []*model.SubData, count int, err error) {
	fields := SubDataColumnsWithID

	sb := db.Select("{fields}").
		From(SubDataTableName)

	if p.FlagForUpdate {
		sb = sb.Suffix("FOR UPDATE")
	}

	if p.SubID != nil {
		sb = sb.Where("dps_sub_id=?", *p.SubID)
	}

	if p.PubID != nil {
		sb = sb.Where("dps_pub_id=?", *p.PubID)
	}

	if p.Type != nil {
		sb = sb.Where("dps_data_type=?", *p.Type)
	}

	if p.ID != nil {
		sb = sb.Where("dps_data_id=?", *p.ID)
	}

	if p.VersionLess != nil {
		sb = sb.Where("dps_sub_data_version<?", *p.VersionLess)
	}

	stmt, bindList, err := sb.ToSql()
	if err != nil {
		return nil, 0, e.W(err, ECode070903)
	}

	if p.FlagCount {
		row := db.QueryRow(strings.Replace(stmt, "{fields}", "count(*)", 1), bindList...)
		if err := row.Scan(&count); err != nil {
			return nil, 0, e.W(err, ECode070904,
				fmt.Sprintf("bindList: %+v", bindList))
		}
	}

	if p.OrderByID != "" {
		sb = sb.OrderBy(fmt.Sprintf("dps_sub_id, dps_pub_id, dps_data_type, dps_data_id %s", p.OrderByID))
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
		return nil, 0, e.W(err, ECode070905)
	}
	defer rows.Close()

	for rows.Next() {
		sq := &model.SubData{}
		if err := rows.Scan(sq.ScanPointers()...); err != nil {
			return nil, 0, e.W(err, ECode070906)
		}

		if p.SubDataHandler != nil {
			if err := p.SubDataHandler(sq); err != nil {
				return nil, 0, e.W(err, ECode070907)
			}
		} else {
			sList = append(sList, sq)
		}
	}

	return sList, count, nil
}

// SubDataGetBySubIDPubIDDataTypeAndDataIDForUpdate retrieves and locks the record for writing. If the
// version in the database is greater, then it does nothing
func SubDataGetBySubIDPubIDDataTypeAndDataIDForUpdate(db *sql.Connection,
	subID, pubID int, dataType, dataID string, version int) (sd *model.SubData, err error) {

	sList, _, err := SubDataGet(db, &SubDataGetParam{
		SubID: &subID,
		PubID: &pubID,
		Type:  &dataType,
		ID:    &dataID,
		// VersionLess:   &version,
		FlagForUpdate: true,
	})
	if err != nil {
		return nil, e.W(err, ECode070908)
	}

	if len(sList) == 0 {
		return nil, e.N(ECode07090E, "not found")
	}

	return sList[0], nil
}

// SubDataNewBulkUpsert initializes and returns a new bulk inserter that will update on conflict
func SubDataNewBulkUpsert(db *sql.Connection) (bi *sql.BulkInsert) {
	// Currently ignoring errors here, as they should only come from sending empty table or columns
	bi, _ = sql.NewBulkInsert(db, SubDataTableName, SubDataColumns, SubDataUpsertOnConflict)
	bi.EnableCache()
	return bi
}

// SubDataGetProcessable
func SubDataGetProcessable(db *sql.Connection, minID, subID, limit int, f func(sd *model.SubData) error) (err error) {
	rows, err := db.Query(stmtSubDataGetProcessable, minID, subID, limit)
	if err != nil {
		return e.W(err, ECode070909)
	}
	defer rows.Close()

	for rows.Next() {
		sd := &model.SubData{
			SubID: subID,
		}
		if err := rows.Scan(&sd.ID, &sd.PubID, &sd.Type, &sd.DataID,
			&sd.Deleted, &sd.Version,
			&sd.Status, &sd.Hash, &sd.Retries); err != nil {
			return e.W(err, ECode07090A)
		}

		if err := f(sd); err != nil {
			return e.W(err, ECode07090B)
		}
	}

	return nil
}

// SubDataCreateMissing creates records from the data table that are missing in the sub data table
func SubDataCreateMissing(db *sql.Connection, subID int) (err error) {
	if _, err := db.Exec(stmtSubDataCreateFromData, subID); err != nil {
		return e.W(err, ECode07090C)
	}

	return nil
}

// SubDataUpdateFromPub udpates the sub data record with the data record.
// If the data record has a higher version, it sets that and marks the sub data record as pending
func SubDataUpdateFromPub(db *sql.Connection, subID int) (err error) {
	if _, err := db.Exec(stmtSubDataUpdateFromData, subID); err != nil {
		return e.W(err, ECode07090D)
	}

	return nil
}
