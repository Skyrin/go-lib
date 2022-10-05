package process

import (
	"fmt"
	"time"

	"github.com/Skyrin/go-lib/e"
	"github.com/Skyrin/go-lib/process/internal/sqlmodel"
	"github.com/Skyrin/go-lib/process/model"
	"github.com/Skyrin/go-lib/sql"
	"github.com/rs/zerolog/log"
)

const (
	ECode030101 = e.Code0301 + "01"
	ECode030102 = e.Code0301 + "02"
	ECode030103 = e.Code0301 + "03"
	ECode030104 = e.Code0301 + "04"
	ECode030105 = e.Code0301 + "05"
	ECode030106 = e.Code0301 + "06"
	ECode030107 = e.Code0301 + "07"
	ECode030108 = e.Code0301 + "08"
	ECode030109 = e.Code0301 + "09"
	ECode03010A = e.Code0301 + "0A"
	ECode03010B = e.Code0301 + "0B"
	ECode03010C = e.Code0301 + "0C"
	ECode03010D = e.Code0301 + "0D"
	ECode03010E = e.Code0301 + "0E"
	ECode03010F = e.Code0301 + "0F"
	ECode03010G = e.Code0301 + "0G"
	ECode03010H = e.Code0301 + "0H"
	ECode03010I = e.Code0301 + "0I"
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
func (p *Processor) Register(code, name string, f func() error) (err error) {
	if err := p.register(code, name, nil, f); err != nil {
		return e.W(err, ECode03010C)
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
func (p *Processor) RegisterWithInterval(code, name string, interval time.Duration, f func() error) (err error) {
	if err := p.register(code, name, &interval, f); err != nil {
		return e.W(err, ECode03010H)
	}

	return nil
}

// register internal function to register a processor. Handles checking if the process already
// exists and creating it if it does not exist
func (p *Processor) register(code, name string, interval *time.Duration, f func() error) (err error) {
	// Only allow registering a code once
	if _, ok := p.runList[code]; ok {
		return e.N(ECode030101,
			fmt.Sprintf("process '%s' already registered", code))
	}

	var id int
	// Check it the process has been created in the process table
	mp, err := sqlmodel.ProcessGetByCode(p.db, code)
	if err != nil {
		if !e.ContainsError(err, sqlmodel.ECode030206_getByCode_notFound) {
			// Return any error except the does not exist by code
			return e.W(err, ECode03010E)
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

		id, err = sqlmodel.ProcessUpsert(p.db, mp)
		if err != nil {
			return e.W(err, ECode030102)
		}
		mp.ID = id
	}

	// Check if the process is active
	if mp.Status != model.ProcessStatusActive {
		return e.N(ECode03010F, "process inactive")
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
func (p *Processor) Deregister(code string) (err error) {
	// Remove the record from the process table
	if err := sqlmodel.ProcessDelete(p.db, code); err != nil {
		return e.W(err, ECode030103)
	}

	// If it is in the runList, remove it
	delete(p.runList, code)

	return nil
}

// Run executes the registered process. If it has not been registered, it
// will return an error. It will return a response indicating if it was skipped.
// If yes, it will include a reason. If no, it will include the run details.
func (p *Processor) Run(code string) (rr *RunResponse, err error) {
	r, ok := p.runList[code]
	if !ok {
		return nil, e.N(ECode030104,
			fmt.Sprintf("process '%s' was not registered", code))
	}

	// Lock the process to this run
	dbLock, err := p.db.BeginReturnDB()
	if err != nil {
		return nil, e.W(err, ECode030105)
	}
	defer dbLock.RollbackIfInTxn()

	rr = &RunResponse{
		Skipped: false,
	}

	// Establish the lock for this process record
	proc, err := sqlmodel.ProcessLock(dbLock, r.process.ID)
	if err != nil {
		switch true {
		case e.ContainsError(err, sqlmodel.ECode03020F_lock_alreadyRunning):
			rr.Skipped = true
			rr.SkipReason = "process already running"
			return rr, nil
		case e.ContainsError(err, sqlmodel.ECode03020G_lock_statusInactive):
			rr.Skipped = true
			rr.SkipReason = "process no longer active"
			return rr, nil
		case e.ContainsError(err, sqlmodel.ECode03020H_lock_notReady):
			rr.Skipped = true
			rr.SkipReason = "process not scheduled to run yet"
			return rr, nil
		}

		// Any other reason is an unexpected failure
		return nil, e.W(err, ECode030106)
	}

	// Set the previous and next run times if it has an interval
	if proc.Interval > 0 {
		if err := sqlmodel.ProcessSetRunTime(dbLock, proc.ID); err != nil {
			return nil, e.W(err, ECode030107)
		}
	}

	// Create a new process run record
	rr.Run, err = sqlmodel.ProcessRunCreate(p.db, proc.ID)
	if err != nil {
		return nil, e.W(err, ECode030108)
	}

	// Track the run time
	now := time.Now()
	if err := r.f(); err != nil {
		// Set the runtime
		rr.Run.RunTime = time.Since(now)
		// Set run status to failed, ignore error as we can't do much if
		// it fails and we want to return the originating error
		if err2 := sqlmodel.ProcessRunFail(p.db, rr.Run.ID, err.Error(), rr.Run.RunTime); err2 != nil {
			log.Warn().Err(e.W(err2, ECode030109))
		}

		return rr, e.W(err, ECode03010A)
	}

	// Set the run time
	rr.Run.RunTime = time.Since(now)

	// Set status of run to completed
	if err := sqlmodel.ProcessRunComplete(p.db, rr.Run.ID, "", rr.Run.RunTime); err != nil {
		rr.Run.Error = err.Error()
		return rr, e.W(err, ECode03010B)
	}

	// Set the processes last successful run time
	if err := sqlmodel.ProcessSetLastSuccess(dbLock, proc.ID, rr.Run.RunTime); err != nil {
		rr.Run.Error = err.Error()
		return rr, e.W(err, ECode03010I)
	}

	// Release the lock on this process
	if err := dbLock.Commit(); err != nil {
		return nil, e.W(err, ECode03010D)
	}

	return rr, nil
}

// SetRunInterval sets the interval, defining how often a process should run.
// It will also reset the next run time to the last run time + the new interval
func (p *Processor) SetRunInterval(code string, interval time.Duration) (err error) {
	if err := sqlmodel.ProcessSetInterval(p.db, p.runList[code].process.ID, interval); err != nil {
		return e.W(err, ECode03010G)
	}

	return nil
}
