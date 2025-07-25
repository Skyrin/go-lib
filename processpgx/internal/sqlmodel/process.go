package sqlmodel

import (
	"context"
	"fmt"
	"time"

	"github.com/Skyrin/go-lib/e"
	"github.com/Skyrin/go-lib/processpgx/model"
	sql "github.com/Skyrin/go-lib/sqlpgx"

	gosql "database/sql"
)

const (
	// ProcessTable
	ProcessTable = "process"

	ECode0A0201                     = e.Code0A02 + "01"
	ECode0A0202                     = e.Code0A02 + "02"
	ECode0A0203                     = e.Code0A02 + "03"
	ECode0A0204                     = e.Code0A02 + "04"
	ECode0A0205                     = e.Code0A02 + "05"
	ECode0A0206_getByCode_notFound  = e.Code0A02 + "06"
	ECode0A0207                     = e.Code0A02 + "07"
	ECode0A0208                     = e.Code0A02 + "08"
	ECode0A0209                     = e.Code0A02 + "09"
	ECode0A020A                     = e.Code0A02 + "0A"
	ECode0A020B                     = e.Code0A02 + "0B"
	ECode0A020C                     = e.Code0A02 + "0C"
	ECode0A020D                     = e.Code0A02 + "0D"
	ECode0A020E                     = e.Code0A02 + "0E"
	ECode0A020F_lock_alreadyRunning = e.Code0A02 + "0F"
	ECode0A020G_lock_statusInactive = e.Code0A02 + "0G"
	ECode0A020H_lock_notReady       = e.Code0A02 + "0H"
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
	IsNextRunTime        bool
}

// ProcessUpsert upsert a record into the process table
func ProcessUpsert(ctx context.Context, db *sql.Connection, p *model.Process) (id int, err error) {
	sb := db.Insert(ProcessTable).
		Columns("process_code", "process_name", "process_status",
			"process_next_run_time", "process_interval",
			"process_message", "created_on", "updated_on").
		Values(p.Code, p.Name, model.ProcessStatusActive,
			p.NextRunTime, p.Interval,
			"", "now()", "now()").
		Suffix(`ON CONFLICT ON CONSTRAINT process__ukey DO UPDATE
		SET process_name=excluded.process_name, updated_on=now()
		RETURNING process_id`)

	id, err = db.ExecInsertReturningID(ctx, sb)
	if err != nil {
		return 0, e.W(err, ECode0A0201)
	}

	return id, nil
}

// ProcessGet fetches records from db
func ProcessGet(ctx context.Context, db *sql.Connection, p *ProcessGetParam) (pList []*model.Process, count int, err error) {
	fields := `process_id, process_code, process_name, process_status,
		process_last_run_time, process_next_run_time, EXTRACT(EPOCH FROM process_interval)::INTEGER,
		process_total_success, EXTRACT(MICROSECONDS FROM process_avg_run_time)::INTEGER,
		process_message, created_on, updated_on`

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

	if p.IsNextRunTime {
		// If there is no interval then always run it, otherwise only run it if the current time is
		// past the process's next run time
		sb = sb.Where("process_next_run_time<NOW()")
	}

	if p.FlagCount {
		// Get the count before applying an offset if there is one
		count, err = db.QueryCount(ctx, sb)
		if err != nil {
			return nil, 0, e.W(err, ECode0A0202)
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
	rows, err := db.ToSQLWFieldAndQuery(ctx, sb, fields)
	if err != nil {
		return nil, 0, e.W(err, ECode0A0203)
	}
	defer rows.Close()

	for rows.Next() {
		d := &model.Process{}
		lrt := &gosql.NullTime{}
		successCount := gosql.NullInt64{}
		var interval, averageRunTime int64
		if err := rows.Scan(&d.ID, &d.Code, &d.Name, &d.Status,
			lrt, &d.NextRunTime, &interval,
			&successCount, &averageRunTime,
			&d.Message, &d.CreatedOn, &d.UpdatedOn); err != nil {
			return nil, 0, e.W(err, ECode0A0204)
		}

		d.Interval = time.Duration(interval) * time.Second
		d.AverageRunTime = time.Duration(averageRunTime) * time.Microsecond // Average run time is extracted as micro seconds

		if lrt.Valid {
			d.LastRunTime = lrt.Time
		}

		if successCount.Valid {
			d.SuccessCount = int(successCount.Int64)
		}

		pList = append(pList, d)
	}

	return pList, count, nil
}

// ProcessGetByCode returns the process record with the specified code
func ProcessGetByCode(ctx context.Context, db *sql.Connection, code string) (p *model.Process, err error) {
	pList, _, err := ProcessGet(ctx, db, &ProcessGetParam{
		Code: &code,
	})
	if err != nil {
		return nil, e.W(err, ECode0A0205)
	}

	if len(pList) == 0 {
		return nil, e.N(ECode0A0206_getByCode_notFound, "unable to find process by code")
	}

	return pList[0], nil
}

// ProcessGetByID returns the process record with the specified id
func ProcessGetByID(ctx context.Context, db *sql.Connection, id int) (p *model.Process, err error) {
	pList, _, err := ProcessGet(ctx, db, &ProcessGetParam{
		ID: &id,
	})
	if err != nil {
		return nil, e.W(err, ECode0A020B)
	}

	if len(pList) == 0 {
		return nil, e.N(ECode0A020C, "process does not exist")
	}

	return pList[0], nil
}

// ProcessLock attempts to establish a lock on the specified process. The process will be skipped
// in the following scenarios:
// 1. The process is already running (the row is already locked)
// 2. The process is no longer active
// 3. The process has an interval and it is not currently past the process's next run time
func ProcessLock(ctx context.Context, db *sql.Connection, id int) (p *model.Process, err error) {
	pList, _, err := ProcessGet(ctx, db, &ProcessGetParam{
		ID:                   &id,
		ForNoKeyUpdateNoWait: true,
		Status:               model.ProcessStatusActive,
		IsNextRunTime:        true,
	})
	if err != nil {
		// Special case if failed due to FOR NO KEY UPDATE NOWAIT
		if e.IsCouldNotLockPQError(err) {
			return nil, e.W(err, ECode0A020F_lock_alreadyRunning)
		}
		return nil, e.W(err, ECode0A020D)
	}

	if len(pList) == 0 {
		// Get the process just by id to try and determine why it failed
		p, err := ProcessGetByID(ctx, db, id)
		if err != nil {
			// Not found at all - unknown cause
			return nil, e.W(err, ECode0A020E)
		}

		// Check if it is still active
		if p.Status != model.ProcessStatusActive {
			return nil, e.N(ECode0A020G_lock_statusInactive, "process is inactive")
		}

		// Otherwise, it should have failed because it is not time to run it yet
		return nil, e.N(ECode0A020H_lock_notReady, "process not ready to run")
	}

	return pList[0], nil
}

// ProcessUpdate updates the specified dock record
func ProcessSetStatusByCode(ctx context.Context, db *sql.Connection, code, status string) (err error) {
	ub := db.Update(ProcessTable).
		Set("process_status", status).
		Set("updated_on", "NOW()").
		Where("process_code = ?", code)

	if err := db.ExecUpdate(ctx, ub); err != nil {
		return e.W(err, ECode0A0207, code, status)
	}

	return nil
}

// ProcessDelete permanently removes the specified record from the process table
func ProcessDelete(ctx context.Context, db *sql.Connection, code string) (err error) {
	delB := db.Delete(ProcessTable).
		Where("process_code", code)

	if err = db.ExecDelete(ctx, delB); err != nil {
		return e.W(err, ECode0A0208)
	}

	return nil
}

// ProcessSetRunTime sets the process's last run time as now and the next run time based on the interval
func ProcessSetRunTime(ctx context.Context, db *sql.Connection, id int) (err error) {
	ub := db.Update(ProcessTable).
		Set("process_last_run_time", db.Expr("NOW()")).
		Set("process_next_run_time", db.Expr("NOW() + process_interval")).
		Where("process_id=?", id)

	if err := db.ExecUpdate(ctx, ub); err != nil {
		return e.W(err, ECode0A0209, fmt.Sprintf("id: %d", id))
	}

	return nil
}

// ProcessSetLastSuccess sets the process's last successful run time as now and updates the process success
// statistics, which include:
//  1. The total number of successful runs
//  2. The average run time
func ProcessSetLastSuccess(ctx context.Context, db *sql.Connection, id int, runTime time.Duration) (err error) {
	const setAvgRunTime = `MAKE_INTERVAL(secs =>
		(COALESCE(EXTRACT(EPOCH FROM process_avg_run_time), 0) * COALESCE(process_total_success,0) + ?)
		/ (COALESCE(process_total_success, 0) + 1)
	)`

	ub := db.Update(ProcessTable).
		Set("process_last_run_time", db.Expr("NOW()")).
		Set("process_total_success", db.Expr("COALESCE(process_total_success, 0) + 1")).
		Set("process_avg_run_time",
			db.Expr(setAvgRunTime, runTime.Seconds())).
		Where("process_id=?", id)

	if err := db.ExecUpdate(ctx, ub); err != nil {
		return e.W(err, ECode0A0209, fmt.Sprintf("id: %d", id))
	}

	return nil
}

// ProcessSetInterval update the interval for the process. Will also set the next run time
// based on the last run time (or now if it does not have a last run time) and the new interval
func ProcessSetInterval(ctx context.Context, db *sql.Connection, id int, interval time.Duration) (err error) {
	ub := db.Update(ProcessTable).
		Set("process_interval", interval.Seconds()).
		Set("process_next_run_time",
			db.Expr("COALESCE(process_last_run_time, NOW()) + MAKE_INTERVAL(secs => ?)", interval.Seconds())).
		Where("process_id=?", id)

	if err := db.ExecUpdate(ctx, ub); err != nil {
		return e.W(err, ECode0A020A, fmt.Sprintf("id: %d, interval: %d", id, interval))
	}

	return nil
}
