package algolia

import (
	"fmt"
	"strings"
	"sync"

	"github.com/Skyrin/go-lib/algolia/model"
	"github.com/Skyrin/go-lib/algolia/sqlmodel"
	"github.com/Skyrin/go-lib/e"
	"github.com/Skyrin/go-lib/sql"
	"github.com/lib/pq"
	"github.com/rs/zerolog/log"
)

const (
	AlgoliaSyncTableName = "algolia_sync"
	AlgoliaDefaultSortBy = "algolia_sync_id"
)

// AlgoliaSyncGetParam model
type AlgoliaSyncGetParam struct {
	Status *[]string
}

// ProccessItemsToPushToAlgolia kicks off the process to push pending and failed items to algolia
func ProccessItemsToPushToAlgolia(db *sql.Connection, alg *Algolia) error {
	// Get all items that are pending to be synced to algolia
	status := []string{model.AlgoliaSyncStatusPending, model.AlgoliaSyncStatusFailed}
	if err := algoliaSyncWithStatus(db, status, alg); err != nil {
		return e.Wrap(err, e.Code0508, "01")
	}

	return nil
}

// pushItemToAlgolia pushes the item to algolia
func pushItemToAlgolia(db *sql.Connection, item *model.AlgoliaSync, alg *Algolia) error {
	var wg sync.WaitGroup
	wg.Add(1)
	go func(item *model.AlgoliaSync) {
		defer wg.Done()

		if err := alg.Push(item); err != nil {
			log.Warn().Err(err).Msg(fmt.Sprintf("%s%s", e.Code0505, "01"))

			// Change status to failed for that item
			if err := sqlmodel.AlgoliaSyncStatusUpdate(db, item.ID,
				model.AlgoliaSyncStatusFailed); err != nil {
				log.Warn().Err(err).Msg(fmt.Sprintf("%s%s", e.Code0505, "02"))
			}

			return
		}

		// Change status to complete for that item
		if err := sqlmodel.AlgoliaSyncStatusUpdate(db, item.ID,
			model.AlgoliaSyncStatusComplete); err != nil {
			log.Warn().Err(err).Msg(fmt.Sprintf("%s%s", e.Code0505, "03"))
		}
	}(item)

	wg.Wait()

	return nil
}

// algoliaSyncGet performs select that pushes directly to algolia
func algoliaSyncGet(db *sql.Connection, p *AlgoliaSyncGetParam,
	alg *Algolia) (err error) {
	fields := `algolia_sync_id, algolia_sync_index, algolia_sync_object_id, algolia_sync_item_id, 
		algolia_sync_item, algolia_sync_item_hash, algolia_sync_status, 
		created_on, updated_on`

	sb := db.Select("{fields}").
		From(AlgoliaSyncTableName)

	if p.Status != nil && len(*p.Status) > 0 {
		sb = sb.Where("algolia_sync_status = ANY(?)", pq.Array(*p.Status))
	}

	stmt, bindList, err := sb.ToSql()
	stmt = strings.Replace(stmt, "{fields}", fields, 1)
	rows, err := db.Query(stmt, bindList...)
	if err != nil {
		return e.Wrap(err, e.Code050A, "01")
	}
	defer rows.Close()

	for rows.Next() {
		as := &model.AlgoliaSync{}
		if err := rows.Scan(&as.ID, &as.AlgoliaIndex, &as.AlgoliaObjectID, &as.ItemID, &as.Item,
			&as.ItemHash, &as.Status, &as.CreatedOn, &as.UpdatedOn); err != nil {
			return e.Wrap(err, e.Code050A, "02")
		}

		pushItemToAlgolia(db, as, alg)
	}

	return nil
}

// algoliaSyncWithStatus searches for items with the specified status that are pushed to algolia
func algoliaSyncWithStatus(db *sql.Connection, status []string, alg *Algolia) (err error) {
	p := &AlgoliaSyncGetParam{
		Status: &status,
	}

	if err := algoliaSyncGet(db, p, alg); err != nil {
		return e.Wrap(err, e.Code050B, "01")
	}

	return nil
}
