package sqlmodel

import (
	"fmt"
	"strings"

	"github.com/Skyrin/go-lib/e"
	"github.com/Skyrin/go-lib/pubsub/model"
	"github.com/Skyrin/go-lib/sql"
)

const (
	PubTableName          = "skyrin_dps_pub"
	PubTableNameWithAlias = PubTableName + " AS p"
	PubColumns            = `dps_pub_id, dps_pub_code, dps_pub_name, dps_pub_status, created_on, updated_on`
	PubColumnsWithAlias   = `p.dps_pub_id, p.dps_pub_code, p.dps_pub_name, p.dps_pub_status, p.created_on, p.updated_on`

	stmtPubJoinPubSubMap = PubSubMapTableName + " AS pbm ON pbm.dps_pub_id=p.dps_pub_id"

	ECode070601 = e.Code0706 + "01"
	ECode070602 = e.Code0706 + "02"
	ECode070603 = e.Code0706 + "03"
	ECode070604 = e.Code0706 + "04"
	ECode070605 = e.Code0706 + "05"
	ECode070606 = e.Code0706 + "06"
	ECode070607 = e.Code0706 + "07"
	ECode070608 = e.Code0706 + "08"
	ECode070609 = e.Code0706 + "09"
	ECode07060A = e.Code0706 + "0A"
	ECode07060B = e.Code0706 + "0B"
)

// PubGetParam model
type PubGetParam struct {
	Limit         *uint64
	Offset        *uint64
	NoLimit       bool
	ID            *int
	Code          *string
	Status        *string
	StatusList    *[]string
	SubID         *int
	Search        *string
	FlagCount     bool
	FlagForUpdate bool
	OrderByID     string
	DataHandler   func(*model.Pub) error
}

// PubInsert inserts a record
func PubInsert(db *sql.Connection, input *model.Pub) (id int, err error) {
	values, err := input.InsertValues()
	if err != nil {
		return 0, e.W(err, ECode070601)
	}

	ib := db.Insert(PubTableName).
		Columns(PubColumns).
		Values(values...).
		Suffix("RETURNING dps_pub_id")

	id, err = db.ExecInsertReturningID(ib)
	if err != nil {
		return 0, e.W(err, ECode070602)
	}

	return id, nil
}

// PubSetStatus updates the status
func PubSetStatus(db *sql.Connection, id int, status string) (err error) {
	ub := db.Update(PubTableName).
		Where("dps_pub_id=?", id).
		Set("dps_pub_status", status).
		Set("updated_on", "now()")

	if err := db.ExecUpdate(ub); err != nil {
		return e.W(err, ECode070603)
	}

	return nil
}

// PubGet performs select
func PubGet(db *sql.Connection,
	p *PubGetParam) (sList []*model.Pub, count int, err error) {
	fields := PubColumnsWithAlias

	sb := db.Select("{fields}").
		From(PubTableNameWithAlias)

	if p.FlagForUpdate {
		sb = sb.Suffix("FOR UPDATE")
	}

	if p.ID != nil {
		sb = sb.Where("p.dps_pub_id=?", *p.ID)
	}

	if p.Code != nil {
		sb = sb.Where("p.dps_pub_code = ?", *p.Code)
	}

	if p.Status != nil {
		sb = sb.Where("p.dps_pub_status = ?", *p.Status)
	}

	if p.Search != nil {
		sb = sb.Where(db.Expr("(p.dps_pub_code like ? OR p.dps_pub_name like ?)", *p.Search, *p.Search))
	}

	if p.SubID != nil {
		sb = sb.Join(stmtPubJoinPubSubMap)
	}

	stmt, bindList, err := sb.ToSql()
	if err != nil {
		return nil, 0, e.W(err, ECode070604)
	}

	if p.FlagCount {
		row := db.QueryRow(strings.Replace(stmt, "{fields}", "count(*)", 1), bindList...)
		if err := row.Scan(&count); err != nil {
			return nil, 0, e.W(err, ECode070605,
				fmt.Sprintf("bindList: %+v", bindList))
		}
	}

	if p.OrderByID != "" {
		sb = sb.OrderBy(fmt.Sprintf("p.dps_pub_id %s", p.OrderByID))
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
		return nil, 0, e.W(err, ECode070606)
	}
	defer rows.Close()

	for rows.Next() {
		sq := &model.Pub{}
		if err := rows.Scan(sq.ScanPointers()...); err != nil {
			return nil, 0, e.W(err, ECode070607)
		}

		if p.DataHandler != nil {
			if err := p.DataHandler(sq); err != nil {
				return nil, 0, e.W(err, ECode070608)
			}
		} else {
			sList = append(sList, sq)
		}
	}

	return sList, count, nil
}

// PubGetBySubID get by sub id
func PubGetBySubID(db *sql.Connection, subID int, f func(*model.Pub) error) (pList []*model.Pub, err error) {
	pList, _, err = PubGet(db, &PubGetParam{
		SubID:       &subID,
		DataHandler: f,
	})
	if err != nil {
		return nil, e.W(err, ECode070609)
	}

	return pList, nil
}

// PubGetByCode get by code
func PubGetByCode(db *sql.Connection, code string) (p *model.Pub, err error) {
	pList, _, err := PubGet(db, &PubGetParam{
		Code:       &code,
	})
	if err != nil {
		return nil, e.W(err, ECode07060A)
	}

	if len(pList) == 0 {
		return nil, e.N(ECode07060B, "publisher does not exist")
	}
	return pList[0], nil
}

