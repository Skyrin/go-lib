package sqlmodel

import (
	"fmt"

	"github.com/Skyrin/go-lib/e"
	"github.com/Skyrin/go-lib/process/model"
	"github.com/Skyrin/go-lib/sql"
)

const (
	// ProcessTable
	ProcessTable = "process"

	ECode030201 = e.Code0302 + "01"
	ECode030202 = e.Code0302 + "02"
	ECode030203 = e.Code0302 + "03"
	ECode030204 = e.Code0302 + "04"
	ECode030205 = e.Code0302 + "05"
	ECode030206 = e.Code0302 + "06"
	ECode030207 = e.Code0302 + "07"
	ECode030208 = e.Code0302 + "08"
)

// ProcessGetParam get params
type ProcessGetParam struct {
	Limit                int
	Offset               int
	ID                   *int
	Code                 *string
	FlagCount            bool
	OrderByID            string
	ForNoKeyUpdateNoWait bool
	Status               string
}

// ProcessUpsert upsert a record into the process table
func ProcessUpsert(db *sql.Connection, p *model.Process) (id int, err error) {
	sb := db.Insert(ProcessTable).
		Columns("process_code", "process_name", "process_status", "process_message", "created_on", "updated_on").
		Values(p.Code, p.Name, model.ProcessStatusActive, "", "now()", "now()").
		Suffix(`ON CONFLICT ON CONSTRAINT process__ukey DO UPDATE
		SET process_name=excluded.process_name, updated_on=now() 
		RETURNING process_id`)

	id, err = db.ExecInsertReturningID(sb)
	if err != nil {
		return 0, e.W(err, ECode030201)
	}

	return id, nil
}

// ProcessGet fetches records from db
func ProcessGet(db *sql.Connection, p *ProcessGetParam) (pList []*model.Process, count int, err error) {
	fields := `process_id, process_code, process_name, process_status, process_message, created_on, updated_on`

	if p.Limit == 0 {
		p.Limit = 1
	}

	sb := db.Select(sql.FieldPlaceHolder).
		From(ProcessTable).
		Limit(uint64(p.Limit))

	if p.ID != nil {
		sb = sb.Where("process_id = ?", *p.ID)
	}

	if p.Code != nil && len(*p.Code) > 0 {
		sb = sb.Where("process_code = ?", *p.Code)
	}

	if p.Status != "" {
		sb = sb.Where("process_status=?", p.Status)
	}

	if p.FlagCount {
		// Get the count before applying an offset if there is one
		count, err = db.QueryCount(sb)
		if err != nil {
			return nil, 0, e.W(err, ECode030202)
		}
	}

	sb = sb.Offset(uint64(p.Offset))

	if p.OrderByID != "" {
		sb = sb.OrderBy(fmt.Sprintf("process_id %s", p.OrderByID))
	}

	if p.ForNoKeyUpdateNoWait {
		sb = sb.Suffix("FOR NO KEY UPDATE NOWAIT")
	}

	// Perform the query
	rows, err := db.ToSQLWFieldAndQuery(sb, fields)
	if err != nil {
		return nil, 0, e.W(err, ECode030203)
	}
	defer rows.Close()

	for rows.Next() {
		d := &model.Process{}
		if err := rows.Scan(&d.ID, &d.Code, &d.Name, &d.Status, &d.Message, &d.CreatedOn, &d.UpdatedOn); err != nil {
			return nil, 0, e.W(err, ECode030204)
		}

		pList = append(pList, d)
	}

	return pList, count, nil
}

// ProcessGetByCode returns the process record with the specified code
func ProcessGetByCode(db *sql.Connection, code string) (p *model.Process, err error) {
	pList, _, err := ProcessGet(db, &ProcessGetParam{
		Code: &code,
	})
	if err != nil {
		return nil, e.W(err, ECode030205)
	}

	if len(pList) == 0 {
		return nil, e.N(ECode030206, "unable to find process by code")
	}

	return pList[0], nil
}

// ProcessUpdate updates the specified dock record
func ProcessSetStatusByCode(db *sql.Connection, code, status string) (err error) {
	ub := db.Update(ProcessTable).
		Set("process_status", status).
		Set("updated_on", "now()").
		Where("process_code = ?", code)

	if err := db.ExecUpdate(ub); err != nil {
		return e.W(err, ECode030207, code, status)
	}

	return nil
}

// ProcessDelete permanently removes the specified record from the process table
func ProcessDelete(db *sql.Connection, code string) (err error) {
	delB := db.Delete(ProcessTable).
		Where("process_code", code)

	if err = db.ExecDelete(delB); err != nil {
		return e.W(err, ECode030208)
	}

	return nil
}
