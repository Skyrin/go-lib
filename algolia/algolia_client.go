// Package algolia provides the necessary calls to communicate with algolia (push, delete)

package algolia

import (
	"fmt"
	"sync"

	"github.com/Skyrin/go-lib/algolia/model"
	"github.com/Skyrin/go-lib/algolia/sqlmodel"
	"github.com/Skyrin/go-lib/e"
	"github.com/Skyrin/go-lib/sql"
	"github.com/algolia/algoliasearch-client-go/v3/algolia/search"
)

const (
	MaxGoroutines        = 1000 // The maximum allowed number of go routines
	DefaultNumGoRoutines = 25
)

// Algolia handler for pushing index updates to Algolia
type Algolia struct {
	Client        *search.Client
	Index         *search.Index
	numGoRoutines int
}

// NewAlgolia initialize a new algolia client/index
func NewAlgolia(config *model.AlgoliaConfig) (alg *Algolia, err error) {
	// Validate all required environment configurations are specified
	if config.App == "" {
		return nil, fmt.Errorf("Algolia App not specified")
	}

	if config.Key == "" {
		return nil, fmt.Errorf("Algolia Key not specified")
	}

	if config.Index == "" {
		return nil, fmt.Errorf("Algolia Index not specified")
	}

	if config.NumGoRoutines > MaxGoroutines {
		return nil, e.New(e.Code050F, "04",
			fmt.Sprintf("Number go routines '%d' exceeds allowed max: %d",
				config.NumGoRoutines, MaxGoroutines))
	} else if config.NumGoRoutines <= 0 {
		config.NumGoRoutines = DefaultNumGoRoutines
	}

	// Initialize the Algolia client
	alg = &Algolia{}
	alg.Client = search.NewClient(config.App, config.Key)
	alg.Index = alg.Client.InitIndex(config.Index)
	alg.numGoRoutines = config.NumGoRoutines

	return alg, nil
}

// Push saves an item to the specified Algolia Index
func (alg *Algolia) Push(item interface{}) (err error) {
	_, err = alg.Index.SaveObject(item)
	if err != nil {
		return e.Wrap(err, e.Code0501, "01")
	}

	return nil
}

// Delete deletes an item from the specified Algolia Index
func (alg *Algolia) Delete(objectID string) (err error) {
	_, err = alg.Index.DeleteObject(objectID)
	if err != nil {
		return e.Wrap(err, e.Code0502, "01")
	}

	return nil
}

// Sync process to send all 'pending' and 'failed' records to algolia
func (alg *Algolia) Sync(db *sql.Connection, f func(*sql.Connection, *model.AlgoliaSync) error) (err error) {
	resCh := make(chan *model.AlgoliaSync, alg.numGoRoutines)
	errCh := make(chan error)
	var wg sync.WaitGroup

	// Start configured number of go routines to process records
	wg.Add(alg.numGoRoutines)
	for i := 0; i < alg.numGoRoutines; i++ {
		go func() {
			defer wg.Done()

			for {
				item, ok := <-resCh
				if !ok {
					// Nothing else to do, so return
					return
				}

				// If an error occured, then return as won't process anymore
				if err := f(db, item); err != nil {
					errCh <- e.Wrap(err, e.Code0509, "01")
					return
				}

				// Send a nil response to indicate no error occurred
				errCh <- nil
			}
		}()
	}

	// Get all items that are pending to be synced to algolia
	p := &sqlmodel.AlgoliaSyncGetParam{
		Status: &[]string{model.AlgoliaSyncStatusPending, model.AlgoliaSyncStatusFailed},
		DataHandler: func(as *model.AlgoliaSync) error {
			// Send to channel for processing - this will limit the number of database
			// connections that are opened as the channel will only allow x number of
			// threads at a time
			resCh <- as

			// If an error was received, then return an error, which will stop fetching
			// In this case, we don't care if the error came from this particular thread,
			// any error will stop the processing
			err := <-errCh
			if err != nil {
				return e.Wrap(err, e.Code0509, "02")
			}
			return nil
		},
	}

	if _, _, err := sqlmodel.AlgoliaSyncGet(db, p); err != nil {
		return e.Wrap(err, e.Code0509, "02")
	}

	// Close the channels
	close(resCh)
	close(errCh)

	// Wait for the threads to complete
	wg.Wait()

	return nil
}

// syncItem sync an item to algolia
func (alg *Algolia) SyncItem(db *sql.Connection, item *model.AlgoliaSync) (err error) {
	// TODO: set the index based on the item (may need to configure/initialize
	// all indexes as part of the new client)
	if item.ForDelete {
		err = alg.Delete(item.AlgoliaObjectID)
	} else {
		err = alg.Push(item)
	}

	if err != nil {
		// Change status to failed for that item
		// TODO: save error to algolia_sync_error_text or something similar
		if err2 := sqlmodel.AlgoliaSyncStatusUpdate(db, item.ID,
			model.AlgoliaSyncStatusFailed); err2 != nil {

			return e.Wrap(err, e.Code0505, "01")
		}

		// Since saved the status/failure, return nil
		return nil
	}

	// Change status to complete for that item
	if err := sqlmodel.AlgoliaSyncStatusUpdate(db, item.ID,
		model.AlgoliaSyncStatusComplete); err != nil {

		return e.Wrap(err, e.Code0505, "02")
	}

	return nil
}
