package process

import (
	"fmt"

	"github.com/Skyrin/go-lib/e"
	"github.com/Skyrin/go-lib/process/internal/sqlmodel"
	"github.com/Skyrin/go-lib/process/model"
	"github.com/Skyrin/go-lib/sql"
	"github.com/rs/zerolog/log"
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
		return e.New(e.Code0609, "01",
			fmt.Sprintf("process '%s' already registered", code))
	}

	// TODO: upsert to process
	mp := &model.Process{
		Code:   code,
		Name:   name,
		Status: model.ProcessStatusReady,
	}

	id, err := sqlmodel.ProcessUpsert(p.db, mp)
	if err != nil {
		return e.Wrap(err, e.Code0609, "02")
	}
	mp.ID = id

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
		return e.Wrap(err, e.Code060C, "01")
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
		return e.New(e.Code060A, "01",
			fmt.Sprintf("process '%s' was not registered", code))
	}

	// Lock the process to this run
	dbLock, err := p.db.BeginReturnDB()
	if err != nil {
		return e.Wrap(err, e.Code060A, "02")
	}
	defer dbLock.RollbackIfInTxn()

	// Establish the lock
	if _, _, err := sqlmodel.ProcessGet(dbLock, &sqlmodel.ProcessGetParam{
		Code:            &r.process.Code,
		ForNoKeyUpdateNoWait: true,
	}); err != nil {
		return e.Wrap(err, e.Code060A, "03")
	}

	// Set status of process to running (this will lock the process)
	if err := sqlmodel.ProcessSetStatusByCode(dbLock, r.process.Code,
		model.ProcessStatusRunning); err != nil {

		return e.Wrap(err, e.Code060A, "04")
	}

	// Create a new process run record
	runID, err := sqlmodel.ProcessRunCreate(p.db, r.process.ID)
	if err != nil {
		return e.Wrap(err, e.Code060A, "05")
	}

	if err := r.f(); err != nil {
		// Set run status to failed, ignore error as we can't do much if
		// it fails and we want to return the originating error
		if err2 := sqlmodel.ProcessRunFail(p.db, runID, err.Error()); err2 != nil {
			log.Warn().Err(e.Wrap(err2, e.Code060A, "0A"))
		}

		return e.Wrap(err, e.Code060A, "06")
	}

	// Set status of run to completed
	if err := sqlmodel.ProcessRunComplete(p.db, runID, ""); err != nil {
		return e.Wrap(err, e.Code060A, "07")
	}

	// Reset the status of the process to ready
	if err := sqlmodel.ProcessSetStatusByCode(dbLock,
		r.process.Code, model.ProcessStatusReady); err != nil {

		return e.Wrap(err, e.Code060A, "08")
	}

	// Release the lock on this process
	if err := dbLock.Commit(); err != nil {
		return e.Wrap(err, e.Code060A, "09")
	}

	return nil
}
