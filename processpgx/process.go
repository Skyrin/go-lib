package processpgx

import (
	"context"
	"fmt"
	"time"

	"github.com/Skyrin/go-lib/e"
	"github.com/Skyrin/go-lib/processpgx/internal/sqlmodel"
	"github.com/Skyrin/go-lib/processpgx/model"
	sql "github.com/Skyrin/go-lib/sqlpgx"
	"github.com/rs/zerolog/log"
)

const (
	ECode0A0101 = e.Code0A01 + "01"
	ECode0A0102 = e.Code0A01 + "02"
	ECode0A0103 = e.Code0A01 + "03"
	ECode0A0104 = e.Code0A01 + "04"
	ECode0A0105 = e.Code0A01 + "05"
	ECode0A0106 = e.Code0A01 + "06"
	ECode0A0107 = e.Code0A01 + "07"
	ECode0A0108 = e.Code0A01 + "08"
	ECode0A0109 = e.Code0A01 + "09"
	ECode0A010A = e.Code0A01 + "0A"
	ECode0A010B = e.Code0A01 + "0B"
	ECode0A010C = e.Code0A01 + "0C"
	ECode0A010D = e.Code0A01 + "0D"
	ECode0A010E = e.Code0A01 + "0E"
	ECode0A010F = e.Code0A01 + "0F"
	ECode0A010G = e.Code0A01 + "0G"
	ECode0A010H = e.Code0A01 + "0H"
	ECode0A010I = e.Code0A01 + "0I"
)

// Processor is used to create a singleton process. It ensures only
// one process is running at a time.
type Processor struct {
	db      *sql.Connection
	runList map[string]*run
}

// RunResponse the response returned after running a process
type RunResponse struct {
	Skipped    bool              // Indicates if skipped
	SkipReason string            // Indicates why it was skipped
	Run        *model.ProcessRun // The run itself
}

type run struct {
	process *model.Process
	f       func() error
}

// NewDataProcess returns a new instance of a processor
func NewProcessor(db *sql.Connection) (p *Processor) {
	return &Processor{
		db: db,
	}
}

// Register will register the process. If the process is already registered, it will
// return an error. If the process does not exist, it will create it. The application
// using this package should register all processes on start to ensure they exist before
// trying to call them.
//
// The run function will be invoked when the process is called later. It
// creates a lock on the process (in the database) to ensure only one
// can run at a time.The run func should define all data processing that
// needs to occur for this run.
func (p *Processor) Register(ctx context.Context, code, name string, f func() error) (err error) {
	if err := p.register(ctx, code, name, nil, f); err != nil {
		return e.W(err, ECode0A010C)
	}

	return nil
}

// RegisterWithInterval will register the process with the specified interval.
// If the process is already registered, it will return an error. If the process
// does not exist, it will create it. The application using this package should
// register all processes on start to ensure they exist before trying to call them.
//
// The run function will be invoked when the process is called later. It
// creates a lock on the process (in the database) to ensure only one
// can run at a time. The run func should define all data processing that
// needs to occur for this process.
func (p *Processor) RegisterWithInterval(ctx context.Context, code, name string, interval time.Duration, f func() error) (err error) {
	if err := p.register(ctx, code, name, &interval, f); err != nil {
		return e.W(err, ECode0A010H)
	}

	return nil
}

// register internal function to register a processor. Handles checking if the process already
// exists and creating it if it does not exist
func (p *Processor) register(ctx context.Context, code, name string, interval *time.Duration, f func() error) (err error) {
	// Only allow registering a code once
	if _, ok := p.runList[code]; ok {
		return e.N(ECode0A0101,
			fmt.Sprintf("process '%s' already registered", code))
	}

	var id int
	// Check it the process has been created in the process table
	mp, err := sqlmodel.ProcessGetByCode(ctx, p.db, code)
	if err != nil {
		if !e.ContainsError(err, sqlmodel.ECode0A0206_getByCode_notFound) {
			// Return any error except the does not exist by code
			return e.W(err, ECode0A010E)
		}
		// The process does not exist yet, create it now
		mp = &model.Process{
			Code:   code,
			Name:   name,
			Status: model.ProcessStatusActive,
		}

		if interval != nil {
			mp.Interval = *interval
		}

		id, err = sqlmodel.ProcessUpsert(ctx, p.db, mp)
		if err != nil {
			return e.W(err, ECode0A0102)
		}
		mp.ID = id
	}

	// Check if the process is active
	if mp.Status != model.ProcessStatusActive {
		return e.N(ECode0A010F, "process inactive")
	}

	r := &run{
		process: mp,
		f:       f,
	}

	if p.runList == nil {
		p.runList = make(map[string]*run, 1)
	}
	p.runList[code] = r

	return nil
}

// Deregister will permanently remove the specified code from the process table
func (p *Processor) Deregister(ctx context.Context, code string) (err error) {
	// Remove the record from the process table
	if err := sqlmodel.ProcessDelete(ctx, p.db, code); err != nil {
		return e.W(err, ECode0A0103)
	}

	// If it is in the runList, remove it
	delete(p.runList, code)

	return nil
}

// Run executes the registered process. If it has not been registered, it
// will return an error. It will return a response indicating if it was skipped.
// If yes, it will include a reason. If no, it will include the run details.
func (p *Processor) Run(ctx context.Context, code string) (rr *RunResponse, err error) {
	r, ok := p.runList[code]
	if !ok {
		return nil, e.N(ECode0A0104,
			fmt.Sprintf("process '%s' was not registered", code))
	}

	// Lock the process to this run
	dbLock, err := p.db.BeginReturnDB(ctx)
	if err != nil {
		return nil, e.W(err, ECode0A0105)
	}
	defer dbLock.RollbackIfInTxn(ctx)

	rr = &RunResponse{
		Skipped: false,
	}

	// Establish the lock for this process record
	proc, err := sqlmodel.ProcessLock(ctx, dbLock, r.process.ID)
	if err != nil {
		switch true {
		case e.ContainsError(err, sqlmodel.ECode0A020F_lock_alreadyRunning):
			rr.Skipped = true
			rr.SkipReason = "process already running"
			return rr, nil
		case e.ContainsError(err, sqlmodel.ECode0A020G_lock_statusInactive):
			rr.Skipped = true
			rr.SkipReason = "process no longer active"
			return rr, nil
		case e.ContainsError(err, sqlmodel.ECode0A020H_lock_notReady):
			rr.Skipped = true
			rr.SkipReason = "process not scheduled to run yet"
			return rr, nil
		}

		// Any other reason is an unexpected failure
		return nil, e.W(err, ECode0A0106)
	}

	// Set the previous and next run times if it has an interval
	if proc.Interval > 0 {
		if err := sqlmodel.ProcessSetRunTime(ctx, dbLock, proc.ID); err != nil {
			return nil, e.W(err, ECode0A0107)
		}
	}

	// Create a new process run record
	rr.Run, err = sqlmodel.ProcessRunCreate(ctx, p.db, proc.ID)
	if err != nil {
		return nil, e.W(err, ECode0A0108)
	}

	// Track the run time
	now := time.Now()
	if err := r.f(); err != nil {
		// Set the runtime
		rr.Run.RunTime = time.Since(now)
		// Set run status to failed, ignore error as we can't do much if
		// it fails and we want to return the originating error
		if err2 := sqlmodel.ProcessRunFail(ctx, p.db, rr.Run.ID, err.Error(), rr.Run.RunTime); err2 != nil {
			log.Warn().Err(e.W(err2, ECode0A0109))
		}

		return rr, e.W(err, ECode0A010A)
	}

	// Set the run time
	rr.Run.RunTime = time.Since(now)

	// Set status of run to completed
	if err := sqlmodel.ProcessRunComplete(ctx, p.db, rr.Run.ID, "", rr.Run.RunTime); err != nil {
		rr.Run.Error = err.Error()
		return rr, e.W(err, ECode0A010B)
	}

	// Set the processes last successful run time
	if err := sqlmodel.ProcessSetLastSuccess(ctx, dbLock, proc.ID, rr.Run.RunTime); err != nil {
		rr.Run.Error = err.Error()
		return rr, e.W(err, ECode0A010I)
	}

	// Release the lock on this process
	if err := dbLock.Commit(ctx); err != nil {
		return nil, e.W(err, ECode0A010D)
	}

	return rr, nil
}

// SetRunInterval sets the interval, defining how often a process should run.
// It will also reset the next run time to the last run time + the new interval
func (p *Processor) SetRunInterval(ctx context.Context, code string, interval time.Duration) (err error) {
	if err := sqlmodel.ProcessSetInterval(ctx, p.db, p.runList[code].process.ID, interval); err != nil {
		return e.W(err, ECode0A010G)
	}

	return nil
}
