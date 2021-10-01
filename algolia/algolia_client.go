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
	// Get all items that are pending to be synced to algolia
	status := []string{model.AlgoliaSyncStatusPending, model.AlgoliaSyncStatusFailed}
	asList, _, err := sqlmodel.AlgoliaSyncGetByStatus(db, status)
	if err != nil {
		return e.Wrap(err, e.Code0505, "01")
	}

	// Begin a transaction
	if err := db.Begin(); err != nil {
		return e.Wrap(err, e.Code0505, "02")
	}
	defer db.RollbackIfInTxn()

	count := 0

	// Loop and push to algolia
	var wg sync.WaitGroup
	for _, item := range asList {
		count++
		wg.Add(1)
		go func(item *model.AlgoliaSync) {
			defer wg.Done()

			if err = alg.Push(item.Item); err != nil {
				log.Warn().Err(err).Msg(fmt.Sprintf("%s%s", e.Code0505, "03"))

				// Change status to failed for that item
				if err := sqlmodel.AlgoliaSyncStatusUpdate(db, item.ID,
					model.AlgoliaSyncStatusFailed); err != nil {
					log.Warn().Err(err).Msg(fmt.Sprintf("%s%s", e.Code0505, "04"))
				}

				return
			}

			// Change status to complete for that item
			if err := sqlmodel.AlgoliaSyncStatusUpdate(db, item.ID,
				model.AlgoliaSyncStatusComplete); err != nil {
				log.Warn().Err(err).Msg(fmt.Sprintf("%s%s", e.Code0505, "05"))
			}
		}(item)
	}

	wg.Wait()

	// Commit
	if err := db.Commit(); err != nil {
		return e.Wrap(err, e.Code0505, "06")
	}

	return nil
}
