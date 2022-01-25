// Package algolia provides the necessary calls to communicate with algolia (push, delete)

package algolia

import (
	"fmt"

	"github.com/Skyrin/go-lib/algolia/model"
	"github.com/Skyrin/go-lib/algolia/sqlmodel"
	"github.com/Skyrin/go-lib/e"
	"github.com/Skyrin/go-lib/sql"
	"github.com/algolia/algoliasearch-client-go/v3/algolia/search"
)

const (
	ECode050101 = e.Code0501 + "01"
	ECode050102 = e.Code0501 + "02"
	ECode050103 = e.Code0501 + "03"
	ECode050104 = e.Code0501 + "04"
	ECode050105 = e.Code0501 + "05"
	ECode050106 = e.Code0501 + "06"
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
		return nil, e.N(ECode050101,
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
		return e.W(err, ECode050102)
	}

	return nil
}

// Delete deletes an item from the specified Algolia Index
func (alg *Algolia) Delete(objectID string) (err error) {
	_, err = alg.Index.DeleteObject(objectID)
	if err != nil {
		return e.W(err, ECode050103)
	}

	return nil
}

// DeleteBy deletes records based on the options, must be valid algolia
// delete by options. If wait is true, it will wait for the task to complete
func (alg *Algolia) DeleteBy(opt ...interface{}) (res search.UpdateTaskRes, err error) {
	res, err = alg.Index.DeleteBy(opt...)
	if err != nil {
		return res, e.W(err, ECode050104)
	}

	return res, nil
}

// Sync attempts to send all 'pending' and 'failed' records to algolia
func (alg *Algolia) Sync(db *sql.Connection, f func(*sql.Connection, *model.AlgoliaSync) error) (err error) {
	return runSync(db, alg, f)
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
		if err2 := sqlmodel.AlgoliaSyncSetStatus(db, item.ID,
			model.AlgoliaSyncStatusFailed, &item.ItemHash, item.Item); err2 != nil {
			return e.W(err, ECode050105)
		}

		// Since saved the status/failure, return nil
		return nil
	}

	// Change status to complete for that item
	if err := sqlmodel.AlgoliaSyncSetStatus(db, item.ID,
		model.AlgoliaSyncStatusComplete, &item.ItemHash, item.Item); err != nil {

		return e.W(err, ECode050106)
	}

	return nil
}
