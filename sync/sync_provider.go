package sync

import (
	"sync"

	"github.com/Skyrin/go-lib/e"
	"github.com/Skyrin/go-lib/sql"
	"github.com/Skyrin/go-lib/sync/model"
	"github.com/Skyrin/go-lib/sync/sqlmodel"
	"github.com/rs/zerolog/log"
)

const (
	MaxGoRoutinesAllowed    = 1000 // The maximum allowed number of go routines
	DefaultMaxNumGoRoutines = 25

	ECode060101 = e.Code0601 + "01"
	ECode060102 = e.Code0601 + "02"
	ECode060103 = e.Code0601 + "03"
	ECode060104 = e.Code0601 + "04"
	ECode060105 = e.Code0601 + "05"
	ECode060106 = e.Code0601 + "06"
)

// Provider interface for the sync services
type Provider interface {
	GetSyncQueueObject(itemID int, syncItemType string) *model.SyncQueue
	HandleItemQueue(db *sql.Connection, item *model.SyncQueue) (err error)
	Send(db *sql.Connection, so *model.SyncQueue) (err error)
}

type Service struct {
	syncProvider Provider
}

// NewService returns an instance of a service provider
func NewService(p Provider) *Service {
	return &Service{
		syncProvider: p,
	}
}

func (s *Service) Process(db *sql.Connection, serviceName string,
	maxGoRoutines int) (err error) {
	count := 0
	err = s.sync(db, serviceName, maxGoRoutines,
		func(db *sql.Connection, item *model.SyncQueue) error {
			err := s.syncProvider.HandleItemQueue(db, item)
			if err != nil {
				return e.W(err, ECode060101)
			}

			count++
			if count%100 == 0 {
				log.Info().Msgf("Synced: %d record(s)", count)
			}
			return nil
		},
	)
	if err != nil {
		return e.W(err, ECode060102)
	}

	log.Info().Msgf("Synced: %d record(s)", count)

	return nil
}

func (s *Service) sync(db *sql.Connection, serviceName string, maxGoRoutines int,
	f func(*sql.Connection, *model.SyncQueue) error) (err error) {

	return s.runSync(db, serviceName, maxGoRoutines, f)
}

// runSync attempts to send all 'pending' and 'failed' records to the service
func (s *Service) runSync(db *sql.Connection, serviceName string, maxGoRoutines int,
	f func(*sql.Connection, *model.SyncQueue) error) (err error) {

	// Used to stop listening if an error is encountered
	done := make(chan struct{})
	defer close(done)

	// Start fetching items and get the item/err channels
	itemCh, errCh := getAllPendingAndFailed(db, serviceName, done)

	// Define the result channel, in our case we only care if it had an error or not as the func
	// 'f' only returns an error
	resCh := make(chan error)

	// Use the configured number of go routines to process all items
	var wg sync.WaitGroup
	wg.Add(maxGoRoutines)
	for i := 0; i < maxGoRoutines; i++ {
		go func() {
			s.handleItem(db, f, done, itemCh, resCh)
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
			return e.W(err, ECode060103)
		}
	}

	// Check for pre-result errors - must occur last as getAllPendingAndFailed
	// could possibly block otherwise
	if err := <-errCh; err != nil {
		return e.W(err, ECode060104)
	}

	return nil
}

// handleItem listens to the item channel and calls the passed in func for each received item
func (s *Service) handleItem(db *sql.Connection, f func(*sql.Connection, *model.SyncQueue) error,
	done <-chan struct{}, itemCh <-chan *model.SyncQueue, resCh chan<- error) {

	for item := range itemCh {
		select {
		case resCh <- f(db, item):
		case <-done:
			return
		}
	}
}

// getAllPendingAndFailed gets all pending and failed records and sends them to the returned item channel
// for processing. If an error occurs, it is sent to the error channel
func getAllPendingAndFailed(db *sql.Connection, service string, done <-chan struct{}) (
	<-chan *model.SyncQueue, <-chan error) {

	itemCh := make(chan *model.SyncQueue)
	errCh := make(chan error, 1)

	limit := uint64(50000)
	// Get all items that are pending to be synced to algolia
	p := &sqlmodel.SyncQueueGetParam{
		Limit: &limit,
		Service: &[]string{service},
		Status:  &[]string{model.SyncQueueStatusPending, model.SyncQueueStatusFailed},
		DataHandler: func(as *model.SyncQueue) error {
			select {
			case itemCh <- as: // Send the record to the item channel
			case <-done:
				return e.N(ECode060105, "data-cancelled")
			}
			return nil
		},
	}
	go func() {
		defer func() {
			close(itemCh)
		}()
		if _, _, err := sqlmodel.SyncQueueGet(db, p); err != nil {
			// Send the error to the error channel
			errCh <- e.W(err, ECode060106)
		}

		// Send nil to the error channel so it is processed
		errCh <- nil
	}()

	return itemCh, errCh
}
