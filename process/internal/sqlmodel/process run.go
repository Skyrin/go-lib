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
			return nil, 0, e.Wrap(err, e.Code0601, "01")
		}
	}

	sb = sb.Offset(uint64(p.Offset))

	if p.OrderByID != "" {
		sb = sb.OrderBy(fmt.Sprintf("dock_code %s", p.OrderByID))
	}

	// Perform the query
	rows, err := db.ToSQLWFieldAndQuery(sb, fields)
	if err != nil {
		return nil, 0, e.Wrap(err, e.Code0601, "02")
	}
	defer rows.Close()

	for rows.Next() {
		d := &model.ProcessRun{}
		if err := rows.Scan(&d.ID, &d.ProcessID, &d.Status, &d.Error, &d.CreatedOn, &d.UpdatedOn); err != nil {
			return nil, 0, e.Wrap(err, e.Code0601, "03")
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
		return 0, e.Wrap(err, e.Code0602, "01")
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
		return e.Wrap(err, e.Code0603, "01")
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
		return e.Wrap(err, e.Code0604, "01")
	}

	return nil
}

// ProcessRunDelete deletes record
func ProcessRunDelete(db *sql.Connection, id int, msg string) (err error) {
	d := db.Delete(ProcessRunTable).
		Where("process_run_id = ?", id)

	stmt, bindList, err := d.ToSql()
	if err != nil {
		return e.Wrap(err, e.Code0605, "01",
			fmt.Sprintf("bind: %+v", bindList))
	}

	if _, err := db.Exec(stmt, bindList...); err != nil {
		return e.Wrap(err, e.Code0605, "02",
			fmt.Sprintf("bindList: %+v", bindList))
	}

	return nil
}
