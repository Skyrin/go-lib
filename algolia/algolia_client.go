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
	"github.com/rs/zerolog/log"
)

// Algolia handler for pushing index updates to Algolia
type Algolia struct {
	Client *search.Client
	Index  *search.Index
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

	// Initialize the Algolia client
	alg = &Algolia{}
	alg.Client = search.NewClient(config.App, config.Key)
	alg.Index = alg.Client.InitIndex(config.Index)

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
func (alg *Algolia) Sync(db *sql.Connection) (err error) {
	var wg sync.WaitGroup

	// Begin a transaction
	if err := db.Begin(); err != nil {
		return e.Wrap(err, e.Code0509, "03")
	}
	defer db.RollbackIfInTxn()

	// Get all items that are pending to be synced to algolia
	p := &sqlmodel.AlgoliaSyncGetParam{
		Status: &[]string{model.AlgoliaSyncStatusPending, model.AlgoliaSyncStatusFailed},
		DataHandler: func(as *model.AlgoliaSync) error {
			wg.Add(1)
			go func(item *model.AlgoliaSync) {
				defer wg.Done()

				if err := alg.syncItem(db, item); err != nil {
					log.Warn().Err(err).Msg(fmt.Sprintf("%s%s", e.Code0509, "01"))
				}
			}(as)

			return nil
		},
	}

	if _, _, err := sqlmodel.AlgoliaSyncGet(db, p); err != nil {
		return e.Wrap(err, e.Code0509, "02")
	}

	wg.Wait()

	// Commit
	if err := db.Commit(); err != nil {
		return e.Wrap(err, e.Code0509, "04")
	}

	return nil
}

// syncItem sync an item to algolia
func (alg *Algolia) syncItem(db *sql.Connection, item *model.AlgoliaSync) (err error) {
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
