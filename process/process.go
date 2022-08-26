package process

import (
	"fmt"

	"github.com/Skyrin/go-lib/e"
	"github.com/Skyrin/go-lib/process/internal/sqlmodel"
	"github.com/Skyrin/go-lib/process/model"
	"github.com/Skyrin/go-lib/sql"
	"github.com/rs/zerolog/log"
)

const (
	ECode030101                       = e.Code0301 + "01"
	ECode030102                       = e.Code0301 + "02"
	ECode030103                       = e.Code0301 + "03"
	ECode030104                       = e.Code0301 + "04"
	ECode030105                       = e.Code0301 + "05"
	ECode030106                       = e.Code0301 + "06"
	ECode030107_processAlreadyRunning = e.Code0301 + "07"
	ECode030108                       = e.Code0301 + "08"
	ECode030109                       = e.Code0301 + "09"
	ECode03010A                       = e.Code0301 + "0A"
	ECode03010B                       = e.Code0301 + "0B"
	// ECode03010C = e.Code0301 + "0C"
	ECode03010D = e.Code0301 + "0D"
	ECode03010E = e.Code0301 + "0E"
	ECode03010F = e.Code0301 + "0F"
)

// Processor is used to create a singleton process. It ensures only
// one process is running at a time.
type Processor struct {
	db      *sql.Connection
	runList map[string]*run
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

// Register will register the process. It will upsert the record into the
// process table and create a reference in the processor to allow running
// later. The application using this package should register all processes
// on start to ensure they exist before trying to call them.
//
// The run function will be invoked when the process is called later. It
// creates a lock on the process (in the database) to ensure only one
// can run at a time.The run func should define all data processing that
// needs to occur for this run.
func (p *Processor) Register(code, name string, f func() error) (err error) {
	// Only allow registering a code once
	if _, ok := p.runList[code]; ok {
		return e.N(ECode030101,
			fmt.Sprintf("process '%s' already registered", code))
	}

	var id int
	// Check it the process has been created in the process table
	mp, err := sqlmodel.ProcessGetByCode(p.db, code)
	if err != nil {
		if !e.ContainsError(err, sqlmodel.ECode030206) {
			// Return any error except the does not exist by code
			return e.W(err, ECode03010E)
		}
		// The process does not exist yet, create it now
		mp = &model.Process{
			Code:   code,
			Name:   name,
			Status: model.ProcessStatusActive,
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
// will return an error
func (p *Processor) Run(code string) (err error) {
	r, ok := p.runList[code]
	if !ok {
		return e.N(ECode030104,
			fmt.Sprintf("process '%s' was not registered", code))
	}

	// Lock the process to this run
	dbLock, err := p.db.BeginReturnDB()
	if err != nil {
		return e.W(err, ECode030105)
	}
	defer dbLock.RollbackIfInTxn()

	// Establish the lock for this process record
	if _, _, err := sqlmodel.ProcessGet(dbLock, &sqlmodel.ProcessGetParam{
		Code:                 &r.process.Code,
		ForNoKeyUpdateNoWait: true,
		Status:               model.ProcessStatusActive,
	}); err != nil {
		// Special case if failed due to FOR NO KEY UPDATE NOWAIT
		if e.IsCouldNotLockPQError(err) {
			return e.W(err, ECode030107_processAlreadyRunning)
		}
		return e.W(err, ECode030106)
	}

	// Create a new process run record
	runID, err := sqlmodel.ProcessRunCreate(p.db, r.process.ID)
	if err != nil {
		return e.W(err, ECode030108)
	}

	if err := r.f(); err != nil {
		// Set run status to failed, ignore error as we can't do much if
		// it fails and we want to return the originating error
		if err2 := sqlmodel.ProcessRunFail(p.db, runID, err.Error()); err2 != nil {
			log.Warn().Err(e.W(err2, ECode030109))
		}

		return e.W(err, ECode03010A)
	}

	// Set status of run to completed
	if err := sqlmodel.ProcessRunComplete(p.db, runID, ""); err != nil {
		return e.W(err, ECode03010B)
	}

	// Release the lock on this process
	if err := dbLock.Commit(); err != nil {
		return e.W(err, ECode03010D)
	}

	return nil
}
