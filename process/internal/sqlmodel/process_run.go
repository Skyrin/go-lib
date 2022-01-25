package sqlmodel

import (
	"fmt"

	"github.com/Skyrin/go-lib/e"
	"github.com/Skyrin/go-lib/process/model"
	"github.com/Skyrin/go-lib/sql"
)

const (
	// ProcessRunTable
	ProcessRunTable = "process_run"

	ECode030301 = e.Code0303 + "01"
	ECode030302 = e.Code0303 + "02"
	ECode030303 = e.Code0303 + "03"
	ECode030304 = e.Code0303 + "04"
	ECode030305 = e.Code0303 + "05"
	ECode030306 = e.Code0303 + "06"
	ECode030307 = e.Code0303 + "07"
)

// ProcessRunGetParam get params
type ProcessRunGetParam struct {
	Limit     int
	Offset    int
	ID        *int
	ProcessID *int
	FlagCount bool
	OrderByID string
}

// ProcessRunGet performs the DB query to return the list of docks
func ProcessRunGet(db *sql.Connection, p *ProcessRunGetParam) (dList []*model.ProcessRun, count int, err error) {
	fields := `process_run_id, process_id, process_run_status, process_run_error, created_on, updated_on`

	if p.Limit == 0 {
		p.Limit = 1
	}

	sb := db.Select(sql.FieldPlaceHolder).
		From(ProcessRunTable).
		Limit(uint64(p.Limit))

	if p.ID != nil {
		sb = sb.Where("process_run_id = ?", *p.ID)
	}

	if p.ProcessID != nil {
		sb = sb.Where("process_id = ?", *p.ProcessID)
	}

	if p.FlagCount {
		// Get the count before applying an offset if there is one
		count, err = db.QueryCount(sb)
		if err != nil {
			return nil, 0, e.W(err, ECode030301)
		}
	}

	sb = sb.Offset(uint64(p.Offset))

	if p.OrderByID != "" {
		sb = sb.OrderBy(fmt.Sprintf("dock_code %s", p.OrderByID))
	}

	// Perform the query
	rows, err := db.ToSQLWFieldAndQuery(sb, fields)
	if err != nil {
		return nil, 0, e.W(err, ECode030302)
	}
	defer rows.Close()

	for rows.Next() {
		d := &model.ProcessRun{}
		if err := rows.Scan(&d.ID, &d.ProcessID, &d.Status, &d.Error, &d.CreatedOn, &d.UpdatedOn); err != nil {
			return nil, 0, e.W(err, ECode030303)
		}

		dList = append(dList, d)
	}

	return dList, count, nil
}

// ProcessRunCreate inserts a new record
func ProcessRunCreate(db *sql.Connection, processID int) (id int, err error) {
	sb := db.Insert(ProcessRunTable).
		Columns("process_id", "process_run_status", "process_run_error", "created_on", "updated_on").
		Values(processID, model.ProcessRunStatusRunning, "", "now()", "now()").
		Suffix("RETURNING process_run_id")
	id, err = db.ExecInsertReturningID(sb)
	if err != nil {
		return 0, e.W(err, ECode030304)
	}

	return id, nil
}

// ProcessRunComplete marks record as completed
func ProcessRunComplete(db *sql.Connection, id int, msg string) (err error) {
	ub := db.Update(ProcessRunTable).
		Set("process_run_status", model.ProcessRunStatusCompleted).
		Set("process_run_error", msg).
		Where("process_run_id = ?", id)

	if err := db.ExecUpdate(ub); err != nil {
		return e.W(err, ECode030305)
	}

	return nil
}

// ProcessRunFail marks record as failed
func ProcessRunFail(db *sql.Connection, id int, msg string) (err error) {
	ub := db.Update(ProcessRunTable).
		Set("process_run_status", model.ProcessRunStatusFailed).
		Set("process_run_error", msg).
		Where("process_run_id = ?", id)

	if err := db.ExecUpdate(ub); err != nil {
		return e.W(err, ECode030301)
	}

	return nil
}

// ProcessRunDelete deletes record
func ProcessRunDelete(db *sql.Connection, id int, msg string) (err error) {
	d := db.Delete(ProcessRunTable).
		Where("process_run_id = ?", id)

	stmt, bindList, err := d.ToSql()
	if err != nil {
		return e.W(err, ECode030306,
			fmt.Sprintf("bind: %+v", bindList))
	}

	if _, err := db.Exec(stmt, bindList...); err != nil {
		return e.W(err, ECode030307,
			fmt.Sprintf("bindList: %+v", bindList))
	}

	return nil
}
