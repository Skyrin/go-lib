package sqlmodel

import (
	"fmt"
	"strings"

	"github.com/Skyrin/go-lib/e"
	"github.com/Skyrin/go-lib/pubsub/model"
	"github.com/Skyrin/go-lib/sql"
)

const (
	SubTableName = "skyrin_dps_sub"
	SubColumns   = `dps_sub_id, dps_sub_code, dps_sub_name, dps_sub_status, dps_sub_retries, created_on, updated_on`

	ECode070701 = e.Code0707 + "01"
	ECode070702 = e.Code0707 + "02"
	ECode070703 = e.Code0707 + "03"
	ECode070704 = e.Code0707 + "04"
	ECode070705 = e.Code0707 + "05"
	ECode070706 = e.Code0707 + "06"
	ECode070707 = e.Code0707 + "07"
	ECode070708 = e.Code0707 + "08"
	ECode070709 = e.Code0707 + "09"
	ECode07070A = e.Code0707 + "0A"
	ECode07070B = e.Code0707 + "0B"
)

// SubGetParam model
type SubGetParam struct {
	Limit         *uint64
	Offset        *uint64
	NoLimit       bool
	ID            *int
	Code          *string
	Status        *string
	StatusList    *[]string
	Search        *string
	FlagCount     bool
	FlagForUpdate bool
	OrderByID     string
	DataHandler   func(*model.Sub) error
}

// SubInsert inserts a record
func SubInsert(db *sql.Connection, input *model.Sub) (id int, err error) {
	values, err := input.InsertValues()
	if err != nil {
		return 0, e.W(err, ECode070701)
	}

	ib := db.Insert(SubTableName).
		Columns(SubColumns).
		Values(values...).
		Suffix("RETURNING dps_sub_id")

	id, err = db.ExecInsertReturningID(ib)
	if err != nil {
		return 0, e.W(err, ECode070702)
	}

	return id, nil
}

// SubSetRetries set the retries
func SubSetRetries(db *sql.Connection, id, retries int) (err error) {
	ub := db.Update(SubTableName).
		Where("dps_sub_id=?", id).
		Set("dps_sub_retries", retries).
		Set("updated_on", "now()")

	if err := db.ExecUpdate(ub); err != nil {
		return e.W(err, ECode070703)
	}

	return nil
}

// SubSetStatus updates the status
func SubSetStatus(db *sql.Connection, id int, status string) (err error) {
	ub := db.Update(SubTableName).
		Where("dps_sub_id=?", id).
		Set("dps_sub_status", status).
		Set("updated_on", "now()")

	if err := db.ExecUpdate(ub); err != nil {
		return e.W(err, ECode070704)
	}

	return nil
}

// SubGet performs select
func SubGet(db *sql.Connection,
	p *SubGetParam) (sList []*model.Sub, count int, err error) {
	fields := SubColumns

	sb := db.Select("{fields}").
		From(SubTableName)

	if p.FlagForUpdate {
		sb = sb.Suffix("FOR UPDATE")
	}

	if p.ID != nil {
		sb = sb.Where("dps_sub_id=?", *p.ID)
	}

	if p.Code != nil {
		sb = sb.Where("dps_sub_code = ?", *p.Code)
	}

	if p.Status != nil {
		sb = sb.Where("dps_sub_status = ?", *p.Status)
	}

	if p.Search != nil {
		sb = sb.Where(db.Expr("(dps_sub_code like ? OR dps_sub_name like ?)", *p.Search, *p.Search))
	}

	stmt, bindList, err := sb.ToSql()
	if err != nil {
		return nil, 0, e.W(err, ECode070705)
	}

	if p.FlagCount {
		row := db.QueryRow(strings.Replace(stmt, "{fields}", "count(*)", 1), bindList...)
		if err := row.Scan(&count); err != nil {
			return nil, 0, e.W(err, ECode070706,
				fmt.Sprintf("bindList: %+v", bindList))
		}
	}

	if p.OrderByID != "" {
		sb = sb.OrderBy(fmt.Sprintf("dps_sub_id %s", p.OrderByID))
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
		return nil, 0, e.W(err, ECode070707)
	}
	defer rows.Close()

	for rows.Next() {
		sq := &model.Sub{}
		if err := rows.Scan(sq.ScanPointers()...); err != nil {
			return nil, 0, e.W(err, ECode070708)
		}

		if p.DataHandler != nil {
			if err := p.DataHandler(sq); err != nil {
				return nil, 0, e.W(err, ECode070709)
			}
		} else {
			sList = append(sList, sq)
		}
	}

	return sList, count, nil
}

// SubGetByCode get by code
func SubGetByCode(db *sql.Connection, code string) (s *model.Sub, err error) {
	sList, _, err := SubGet(db, &SubGetParam{
		Code: &code,
	})
	if err != nil {
		return nil, e.W(err, ECode07070A)
	}

	if len(sList) != 1 {
		return nil, e.N(ECode07070B, "not found")
	}

	return sList[0], nil
}
