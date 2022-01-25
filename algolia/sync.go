// Package algolia provides the necessary calls to communicate with algolia (push, delete)

package algolia

import (
	"sync"

	"github.com/Skyrin/go-lib/algolia/model"
	"github.com/Skyrin/go-lib/algolia/sqlmodel"
	"github.com/Skyrin/go-lib/e"
	"github.com/Skyrin/go-lib/sql"
)

const (
	MaxGoroutines        = 1000 // The maximum allowed number of go routines
	DefaultNumGoRoutines = 25
)

const (
	ECode050201 = e.Code0502 + "01"
	ECode050202 = e.Code0502 + "02"
	ECode050203 = e.Code0502 + "03"
	ECode050204 = e.Code0502 + "04"
)

// getAllPendingAndFailed gets all pending and failed records and sends them to the returned item channel
// for processing. If an error occurs, it is sent to the error channel
func getAllPendingAndFailed(db *sql.Connection, done <-chan struct{}) (
	<-chan *model.AlgoliaSync, <-chan error) {

	itemCh := make(chan *model.AlgoliaSync)
	errCh := make(chan error, 1)

	// Get all items that are pending to be synced to algolia
	p := &sqlmodel.AlgoliaSyncGetParam{
		Status: &[]string{model.AlgoliaSyncStatusPending, model.AlgoliaSyncStatusFailed},
		DataHandler: func(as *model.AlgoliaSync) error {
			select {
			case itemCh <- as: // Send the record to the item channel
			case <-done:
				return e.N(ECode050201, "data-cancelled")
			}
			return nil
		},
	}
	go func() {
		defer func() {
			close(itemCh)
		}()
		if _, _, err := sqlmodel.AlgoliaSyncGet(db, p); err != nil {
			// Send the error to the error channel
			errCh <- e.W(err, ECode050202)
		}

		// Send nil to the error channel so it is processed
		errCh <- nil
	}()

	return itemCh, errCh
}

// handleItem listens to the item channel and calls the passed in func for each received item
func handleItem(db *sql.Connection, f func(*sql.Connection, *model.AlgoliaSync) error,
	done <-chan struct{}, itemCh <-chan *model.AlgoliaSync, resCh chan<- error) {

	for item := range itemCh {
		select {
		case resCh <- f(db, item):
		case <-done:
			return
		}
	}
}

// runSync attempts to send all 'pending' and 'failed' records to algolia
func runSync(db *sql.Connection, alg *Algolia,
	f func(*sql.Connection, *model.AlgoliaSync) error) (err error) {

	// Used to stop listening if an error is encountered
	done := make(chan struct{})
	defer close(done)

	// Start fetching items and get the item/err channels
	itemCh, errCh := getAllPendingAndFailed(db, done)

	// Define the result channel, in our case we only care if it had an error or not as the func
	// 'f' only returns an error
	resCh := make(chan error)

	// Use the configured number of go routines to process all items
	var wg sync.WaitGroup
	wg.Add(alg.numGoRoutines)
	for i := 0; i < alg.numGoRoutines; i++ {
		go func() {
			handleItem(db, f, done, itemCh, resCh)
			wg.Done()
		}()
	}
	go func() {
		// Wait until all items have been processed, then close the item result channel
		wg.Wait()
		close(resCh)
	}()

	// Check for any errors in the results
	for err := range resCh {
		if err != nil {
			return e.W(err, ECode050203)
		}
	}

	// Check for pre-result errors - must occur last as getAllPendingAndFailed
	// could possibly block otherwise
	if err := <-errCh; err != nil {
		return e.W(err, ECode050204)
	}

	return nil
}
