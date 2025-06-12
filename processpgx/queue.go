package processpgx

import (
	"sync"

	"github.com/Skyrin/go-lib/e"
)

const (
	ECode0A0401 = e.Code0A04 + "01"
	ECode0A0402 = e.Code0A04 + "02"
	ECode0A0403 = e.Code0A04 + "03"
	ECode0A0404 = e.Code0A04 + "04"
	ECode0A0405 = e.Code0A04 + "05"
	ECode0A0406 = e.Code0A04 + "06"
	ECode0A0407 = e.Code0A04 + "07"
	ECode0A0408 = e.Code0A04 + "08"
	ECode0A0409 = e.Code0A04 + "09"
	ECode0A040A = e.Code0A04 + "0A"
	ECode0A040B = e.Code0A04 + "0B"
	ECode0A040C = e.Code0A04 + "0C"
	ECode0A040D = e.Code0A04 + "0D"
	ECode0A040E = e.Code0A04 + "0E"
	ECode0A040F = e.Code0A04 + "0F"
	ECode0A040G = e.Code0A04 + "0G"
)

// QueueGenerator defines how the queue gets populated and how each record is handled
type QueueGenerator interface {
	// SetQueue is called in the NewQueue func. It should assign the queue to a property
	// of the QueueGenerator, so that the FillQueue func of the generator can call the queue's
	// Add func
	SetQueue(*Queue)
	// FillQueue defines how the queue is populated. It must retrieve the records to be
	// added and call the Queue's Add func to add the records to the queue.
	FillQueue() error
	// HandleQueueItem defines what to do with each record in the queue. It will
	// be called as the queue is read
	HandleQueueItem(item interface{}) (err error)
}

// Queue used to process data in parallel using a queue to limit the number of go routines
// allowed to run at a given time. A function to get the data (getQueue) and a function to
// handle each record of the getQueue must be set.
type Queue struct {
	MaxGoRoutines uint // Defines the maximum number of go routines to run at a time
	wg            sync.WaitGroup
	doneCh        chan struct{}
	errCh         chan error
	queueCh       chan interface{}
	resCh         chan error
	generator     QueueGenerator
}

type QueueConfig struct {
	Generator     QueueGenerator
	MaxGoRoutines uint
}

// NewQueue creates a new queue to run data processing in parallel and uses the max queue to
// define the maximum number of go routines allowed
func NewQueue(qc *QueueConfig) (q *Queue) {
	if qc.MaxGoRoutines == 0 {
		qc.MaxGoRoutines = 1
	}

	q = &Queue{
		MaxGoRoutines: qc.MaxGoRoutines,
		doneCh:        make(chan struct{}),
		errCh:         make(chan error, 1),
		queueCh:       make(chan interface{}),
		resCh:         make(chan error),
		generator:     qc.Generator,
	}

	q.generator.SetQueue(q)

	return q
}

// Add adds the passed item to the queue. This func must be called in the QueueGenerator's
// FillQueue in order to populate the queue.
func (q *Queue) Add(item interface{}) (err error) {
	select {
	case q.queueCh <- item:
	case <-q.doneCh:
		return e.N(ECode0A0401, "queue cancelled")
	}

	return nil
}

// Run processes the queue, calling the queue generator's FillQueue method to get all of the
// records to process and the generator's HandleQueueItem to process each record in the queue.
func (q *Queue) Run() (err error) {
	go func() {
		defer close(q.queueCh)

		if err := q.generator.FillQueue(); err != nil {
			// Send the error to the error channel
			q.errCh <- e.W(err, ECode0A040E)
			return
		}

		// Send nil to the error channel so it is processed
		q.errCh <- nil
	}()

	// Create a limited number of go routines to process the queue
	q.wg.Add(int(q.MaxGoRoutines))
	for i := uint(0); i < q.MaxGoRoutines; i++ {
		go func() {
			for item := range q.queueCh {
				select {
				case q.resCh <- q.generator.HandleQueueItem(item):
				case <-q.doneCh:
					return
				}
			}
			q.wg.Done()
		}()
	}
	go func() {
		// Wait until all records have been processed, then close the channels
		// Note, the queue channel is closed in the getQueue method
		q.wg.Wait()
		q.close()
	}()

	// Check for errors in the results channel
	for err := range q.resCh {
		if err != nil {
			return e.W(err, ECode0A040F)
		}
	}

	// Check for pre-result errors - must occur last as fillQueue
	// could possibly block otherwise
	for err := range q.errCh {
		if err != nil {
			return e.W(err, ECode0A040G)
		}
	}

	return nil
}

// close closes the done, err, and res channels
func (q *Queue) close() {
	close(q.doneCh)
	close(q.errCh)
	close(q.resCh)
}
