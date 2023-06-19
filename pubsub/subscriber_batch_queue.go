package pubsub

import (
	"sync"

	"github.com/Skyrin/go-lib/e"
	"github.com/Skyrin/go-lib/pubsub/internal/sqlmodel"
)

const (
	// Error constants
	ECode070B01 = e.Code070B + "01"
	ECode070B02 = e.Code070B + "02"
	ECode070B03 = e.Code070B + "03"
	ECode070B04 = e.Code070B + "04"
	ECode070B05 = e.Code070B + "05"
)

// runParallelResult a result from runParallel
type runParallelResult struct {
	retrievedCount int
	err            error
}

// runParallel retrieves and locks batches of sub data records. Then, pushes those records to the
// subscriber. Depending on if the push succeeded or failed, it updates the status of the batch 
// records accordingly.
func (s *Subscriber) runParallel(limit int) (rr *runParallelResult) {
	rr = &runParallelResult{}

	txn, err := s.db.BeginReturnDB()
	if err != nil {
		rr.err = e.W(err, ECode070B01)
		return rr
	}

	sb := &subBatch{
		s:      s,
		h:      s.batchHandler,
		db:     txn,
		bu:     sqlmodel.NewSubDataBulk(txn),
		lastID: s.lastID,
	}
	defer func() {
		txn.RollbackIfInTxn()
		sb.close()
	}()

	// Fetch the sub data records and lock them for update
	if err := sqlmodel.SubDataGetProcessable(txn, sb.lastID, sb.s.sub.ID, limit, sb.add); err != nil {
		rr.err = e.W(err, ECode070B02)
		return rr
	}

	// Push the sub data
	if err := sb.push(); err != nil {
		rr.err = e.W(err, ECode070B03)
		return rr
	}

	// Commit
	if err := txn.Commit(); err != nil {
		rr.err = e.W(err, ECode070B04)
		return rr
	}

	// Increment the counts
	s.incrementStats(sb)

	rr.retrievedCount = sb.retrievedCount
	return rr
}

// processor reads from the fill channel and fetches data for it, then sends the result to the result channel
func (s *Subscriber) processor(doneCh <-chan struct{}, fillCh <-chan int, resultCh chan<- *runParallelResult) {
	for batchSize := range fillCh {
		select {
		case resultCh <- s.runParallel(batchSize):
		case <-doneCh:
			// Finish processing
			return
		}
	}
}

// loopParallel retrieves sub data records in batches and pushes those records to the 
// subscriber. It repeats this until the specified batch limit is reached.
func (s *Subscriber) loopParallel(batchSize, batchLimit int) (err error) {
	wg := sync.WaitGroup{}
	doneCh := make(chan struct{})
	defer close(doneCh)

	// Create a channel to fill with data requests
	fillCh := make(chan int)

	// The stop channel is used to stop the fill channel early in the event
	// that no more records are being retrieved from the processable sub data
	stopCh := make(chan struct{})

	// Flag to indicate if the stop channel has been closed
	stopChClosed := false
	defer func() {
		// This could happen if an error occured
		if !stopChClosed {
			close(stopCh)
		}
	}()

	// Push all data into the fill channel
	go func(fillCh chan<- int, stopCh <-chan struct{}, batchSize, batchLimit int) {
		defer close(fillCh)
		curLimit := 0
		for {
			// Process up to the batchLimit
			curLimit += batchSize
			if curLimit >= batchLimit {
				return
			}

			select {
			case fillCh <- batchSize:
			case <-stopCh:
				// Stopping early
				return
			}
		}
	}(fillCh, stopCh, batchSize, batchLimit)

	// Spawn go routines to read from the fill channel and fetch data/push into the result channel
	resultCh := make(chan *runParallelResult, s.maxGoRoutines)
	wg.Add(int(s.maxGoRoutines))
	for i := 0; i < int(s.maxGoRoutines); i++ {
		go func() {
			s.processor(doneCh, fillCh, resultCh)
			wg.Done()
		}()
	}
	go func() {
		// Wait for all processors to complete then close the result channel
		wg.Wait()
		close(resultCh)
	}()

	// Read from the result channel until it is closed (or an error occurs)
	for rr := range resultCh {
		if rr.err != nil {
			return e.W(rr.err, ECode070B05)
		}

		if rr.retrievedCount == 0 {
			if !stopChClosed {
				stopChClosed = true

				// End early if no more records were retrieved
				close(stopCh)
			}
		}
	}
	return nil
}
