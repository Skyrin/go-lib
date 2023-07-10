package pubsub

import (
	"sync"

	"github.com/Skyrin/go-lib/e"
	"github.com/Skyrin/go-lib/pubsub/internal/sqlmodel"
	"github.com/Skyrin/go-lib/pubsub/model"
	"github.com/Skyrin/go-lib/sql"
	"github.com/lib/pq"
)

const (
	defaultMaxGoRoutines = 1

	// Error constants
	ECode070201 = e.Code0702 + "01"
	ECode070202 = e.Code0702 + "02"
)

// Subscriber use NewSubscriber to initialize and either listen for pub data or process
// new/updated pub data in the skyrin_dps_pub table
type Subscriber struct {
	db             *sql.Connection
	sub            *model.Sub
	pubList        []*model.Pub // List of publishers this subscriber is linked with
	listener       *pq.Listener
	h              SubDataListener
	errCh          chan error
	doneCh         chan struct{}
	errHandler     func(err error)
	batchHandler   SubBatchHandler // Called for each sub data records during a batch run
	successCount   int             // Count of succeeded, status set to completed
	failRetryCount int             // Count of failed, but will retry, status kept as pending
	failCount      int             // Count of failed and exceed retry limit, status set to failed
	lastID         int             // The id of the last processable sub data record that was processed
	mutex          sync.RWMutex    // Lock for updating counts/lastID
	maxGoRoutines  uint            // The maximum number of go routines to use while processing data
}

// NewSubscriber initializes the subscriber and processes any pending sub data records
func NewSubscriber(db *sql.Connection, code string) (s *Subscriber, err error) {
	s = &Subscriber{
		db:            db,
		maxGoRoutines: defaultMaxGoRoutines,
		mutex:         sync.RWMutex{},
	}

	s.sub, err = sqlmodel.SubGetByCode(db, code)
	if err != nil {
		return s, e.W(err, ECode070201)
	}

	// Get associated publishers so will only trigger for data from those publishers
	s.pubList, err = sqlmodel.PubGetBySubID(db, s.sub.ID, nil)
	if err != nil {
		return nil, e.W(err, ECode070202)
	}

	// TODO: add pg notify of pub list changes

	return s, nil
}

// SetErrorHandler sets the error handler. If a pubsub fails after the subscriber's configured number of retries,
// this will be called if set when the record is marked as failed.
func (s *Subscriber) SetErrorHandler(f func(err error)) {
	s.errHandler = f
}

// SetMaxGoRoutines set the maximum number of go routines to use while processing
// the subscriber data
func (s *Subscriber) SetMaxGoRoutines(max uint) {
	s.maxGoRoutines = max
}

// incrementStats increments the stats and last processed id if it is greater than
// the currently set last id
func (s *Subscriber) incrementStats(sb *subBatch) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	s.successCount += sb.successCount
	s.failRetryCount += sb.failRetryCount
	s.failCount += sb.failCount

	// This can be called from parallel processes. Only update it if it is greater
	// then the currently set lastID
	if sb.lastID > s.lastID {
		s.lastID = sb.lastID
	}
}
