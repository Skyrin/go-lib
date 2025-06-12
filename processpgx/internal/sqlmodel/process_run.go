package sqlmodel

import (
	"context"
	"fmt"
	"time"

	"github.com/Skyrin/go-lib/e"
	"github.com/Skyrin/go-lib/processpgx/model"
	sql "github.com/Skyrin/go-lib/sqlpgx"
)

const (
	// ProcessRunTable
	ProcessRunTable = "process_run"

	ECode0A0301 = e.Code0A03 + "01"
	ECode0A0302 = e.Code0A03 + "02"
	ECode0A0303 = e.Code0A03 + "03"
	ECode0A0304 = e.Code0A03 + "04"
	ECode0A0305 = e.Code0A03 + "05"
	ECode0A0306 = e.Code0A03 + "06"
	ECode0A0307 = e.Code0A03 + "07"
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
func ProcessRunGet(ctx context.Context, db *sql.Connection, p *ProcessRunGetParam) (dList []*model.ProcessRun, count int, err error) {
	fields := `process_run_id, process_id, process_run_status,
		EXTRACT(EPOCH FROM process_run_time)::INTEGER, process_run_error,
		created_on, updated_on`

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
		count, err = db.QueryCount(ctx, sb)
		if err != nil {
			return nil, 0, e.W(err, ECode0A0301)
		}
	}

	sb = sb.Offset(uint64(p.Offset))

	if p.OrderByID != "" {
		sb = sb.OrderBy(fmt.Sprintf("dock_code %s", p.OrderByID))
	}

	// Perform the query
	rows, err := db.ToSQLWFieldAndQuery(ctx, sb, fields)
	if err != nil {
		return nil, 0, e.W(err, ECode0A0302)
	}
	defer rows.Close()

	for rows.Next() {
		d := &model.ProcessRun{}
		var runTime int64
		if err := rows.Scan(&d.ID, &d.ProcessID,
			&d.Status, &runTime,
			&d.Error, &d.CreatedOn, &d.UpdatedOn); err != nil {

			return nil, 0, e.W(err, ECode0A0303)
		}

		d.RunTime = time.Duration(runTime) * time.Second

		dList = append(dList, d)
	}

	return dList, count, nil
}

// ProcessRunCreate inserts a new record
func ProcessRunCreate(ctx context.Context, db *sql.Connection, processID int) (pr *model.ProcessRun, err error) {
	now := time.Now()
	pr = &model.ProcessRun{
		ProcessID: processID,
		Status:    model.ProcessRunStatusRunning,
		CreatedOn: now,
		UpdatedOn: now,
	}
	sb := db.Insert(ProcessRunTable).
		Columns("process_id", "process_run_status", "process_run_time", "process_run_error", "created_on", "updated_on").
		Values(pr.ProcessID, pr.Status, pr.RunTime.Seconds(), pr.Error, pr.CreatedOn, pr.UpdatedOn).
		Suffix("RETURNING process_run_id")
	pr.ID, err = db.ExecInsertReturningID(ctx, sb)
	if err != nil {
		return nil, e.W(err, ECode0A0304)
	}

	return pr, nil
}

// ProcessRunComplete marks record as completed
func ProcessRunComplete(ctx context.Context, db *sql.Connection, id int, msg string, runTime time.Duration) (err error) {
	ub := db.Update(ProcessRunTable).
		Set("process_run_status", model.ProcessRunStatusCompleted).
		Set("process_run_time", runTime.Seconds()).
		Set("process_run_error", msg).
		Set("updated_on", db.Expr("NOW()")).
		Where("process_run_id = ?", id)

	if err := db.ExecUpdate(ctx, ub); err != nil {
		return e.W(err, ECode0A0305)
	}

	return nil
}

// ProcessRunFail marks record as failed
func ProcessRunFail(ctx context.Context, db *sql.Connection, id int, msg string, runTime time.Duration) (err error) {
	ub := db.Update(ProcessRunTable).
		Set("process_run_status", model.ProcessRunStatusFailed).
		Set("process_run_time", runTime.Seconds()).
		Set("process_run_error", msg).
		Set("updated_on", "NOW()").
		Where("process_run_id = ?", id)

	if err := db.ExecUpdate(ctx, ub); err != nil {
		return e.W(err, ECode0A0301)
	}

	return nil
}

// ProcessRunDelete deletes record
func ProcessRunDelete(ctx context.Context, db *sql.Connection, id int, msg string) (err error) {
	d := db.Delete(ProcessRunTable).
		Where("process_run_id = ?", id)

	stmt, bindList, err := d.ToSql()
	if err != nil {
		return e.W(err, ECode0A0306,
			fmt.Sprintf("bind: %+v", bindList))
	}

	if _, err := db.Exec(ctx, stmt, bindList...); err != nil {
		return e.W(err, ECode0A0307,
			fmt.Sprintf("bindList: %+v", bindList))
	}

	return nil
}
