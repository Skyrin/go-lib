package pubsub

import (
	"github.com/Skyrin/go-lib/e"
	"github.com/Skyrin/go-lib/pubsub/internal/sqlmodel"
	"github.com/Skyrin/go-lib/pubsub/model"
	"github.com/Skyrin/go-lib/sql"
)

const (
	// Error constants
	ECode070A01 = e.Code070A + "01"
	ECode070A02 = e.Code070A + "02"
	ECode070A03 = e.Code070A + "03"
	ECode070A04 = e.Code070A + "04"
	ECode070A05 = e.Code070A + "05"
	ECode070A06 = e.Code070A + "06"
	ECode070A07 = e.Code070A + "07"
	ECode070A08 = e.Code070A + "08"
	ECode070A09 = e.Code070A + "09"
	ECode070A0A = e.Code070A + "0A"
)

// subBatch use to process pending subscriber data records
type subBatch struct {
	db             *sql.Connection
	s              *Subscriber
	h              SubBatchHandler       // Called for each sub data record retrieved
	list           []*Event              // Current batch of sub data to push
	bu             *sqlmodel.SubDataBulk // Bulk update for setting status of sub data
	successCount   int                   // Count of succeeded, status set to completed
	failRetryCount int                   // Count of failed, but will retry, status kept as pending
	failCount      int                   // Count of failed and exceed retry limit, status set to failed
	lastID         int                   // The id of the last processable sub data record that was fetched
	retrievedCount int                   // Tracks the number of records retrieved in each loop
}

// SubBatchHandler defines the logic to send the publish events for the specific subscriber.
type SubBatchHandler interface {
	// Push is called for each batch of sub data to send to a subscriber. The subscriber should load and process
	// the data as needed. If an error is returned, all the pending records will be marked with this error and
	// their retries will be incremented. If any record has reached its retry limit, it will be marked as failed
	// and will not be pushed again until it is changed or it is manually reset to pending.
	Push([]*Event) (err error)
}

// RunBatch runs the batch process for the specified number of records (batchLimit)
func (s *Subscriber) RunBatch(sbh SubBatchHandler, batchSize, batchLimit int) (err error) {
	s.batchHandler = sbh

	// Create/update records for the sub from the pub
	if err := s.createMissingAndUpdateExisting(); err != nil {
		return e.W(err, ECode070A01)
	}

	// Contiuously loop until batch limit is reached
	if err := s.loop(batchSize, batchLimit); err != nil {
		return e.W(err, ECode070A02)
	}

	return nil
}

// GetStats returns info on how many records have been processed.
// total = total number of records
// success = total number of successfully sent records
// retry = total number of failed records, but have been marked to retry
// fail = total number of failed records (retry limit has been exceeded and it will not automatically retry again)
func (s *Subscriber) GetStats() (total, success, retry, fail int) {
	return s.successCount + s.failRetryCount + s.failCount, s.successCount, s.failRetryCount, s.failCount
}

// close closes/cleanup
func (sb *subBatch) close() {
	sb.list = nil
	_ = sb.bu.Close()
}

// loop retrieves ending sub data records in batches and pushes those records to the
// subscriber. It repeats this until the specified batch limit is reached.
func (s *Subscriber) loop(batchSize, batchLimit int) (err error) {
	var total, curLimit, lastID int

	for {
		// If remaining records is less than the batchSize, then set limit to remaining records
		if batchLimit-total < batchSize {
			curLimit = batchLimit - total
		} else {
			curLimit = batchSize
		}

		// run the batch, returns the number of records retrieved and the latest id processed
		retrievedCount, newLastID, err := s.run(lastID, curLimit)
		if err != nil {
			return e.W(err, ECode070A03)
		}

		// Set new latest id
		lastID = newLastID

		// Increment total count
		total += retrievedCount

		// If no more records were retrieved or total has reached the batchLimit, then stop
		if retrievedCount == 0 || total >= batchLimit {
			break
		}
	}

	return nil
}

// run retrieves and locks the batch of sub data records. Then, pushes those records to the subscriber.
// Depending on if the push succeeded or failed, it updates the status of the batch records accordingly.
func (s *Subscriber) run(lastID, limit int) (retrievedCount, newLlastID int, err error) {
	txn, err := s.db.BeginReturnDB()
	if err != nil {
		return 0, lastID, e.W(err, ECode070A04)
	}

	sb := &subBatch{
		s:      s,
		h:      s.batchHandler,
		db:     txn,
		bu:     sqlmodel.NewSubDataBulk(txn),
		lastID: lastID,
	}
	defer func() {
		txn.RollbackIfInTxn()
		sb.close()
	}()

	// Fetch the sub data records and lock them for update
	if err := sqlmodel.SubDataGetProcessable(txn, sb.lastID, sb.s.sub.ID, limit, sb.add); err != nil {
		return 0, lastID, e.W(err, ECode070A05)
	}

	// Push the sub data
	if err := sb.push(); err != nil {
		return 0, lastID, e.W(err, ECode070A06)
	}

	// Commit
	if err := txn.Commit(); err != nil {
		return 0, lastID, e.W(err, ECode070A07)
	}

	// Increment the counts
	s.successCount += sb.successCount
	s.failRetryCount += sb.failRetryCount
	s.failCount += sb.failCount

	return sb.retrievedCount, sb.lastID, nil
}

// add converts the sub data into a Notify object and caches it to be pusehd later
func (sb *subBatch) add(sd *model.SubData) (err error) {
	sb.lastID = sd.ID   // Set the latest id
	sb.retrievedCount++ // Increment the retrieved count

	// Build the notify object
	ev := &Event{
		sd:           sd,
		PubID:        sd.PubID,
		Type:         sd.Type,
		ID:           sd.DataID,
		Deleted:      sd.Deleted,
		Version:      sd.Version,
		PreviousHash: sd.Hash,
	}

	sb.list = append(sb.list, ev)

	return nil
}

// push calls the configured Push and commits the associated pending sub data records. If the push call returns an
// error, it will mark all pending records, that previously succeeded with the new error (incrementing the retry
// and setting the status to failed when appropriate). It will then commit the status for the pending records.
func (sb *subBatch) push() (err error) {
	if len(sb.list) == 0 {
		// Nothing to push
		return nil
	}

	pushErr := sb.h.Push(sb.list)
	// Iterate through the list, and set appropriate status based on if the push succeeded/failed
	for i := range sb.list {
		n := sb.list[i]
		n.sd.SetResponse(n.NewHash, n.NewJSON, pushErr, n.Version, n.Deleted, sb.s.sub)

		// Increment count of success/retry/fail
		switch n.sd.Status {
		case model.SubDataStatusCompleted:
			sb.successCount++
		case model.SubDataStatusPending:
			sb.failRetryCount++
		case model.SubDataStatusFailed:
			sb.failCount++
		}
	}

	// Commit the status of the pending records
	for i := range sb.list {
		if _, err := sb.bu.Add(sb.list[i].sd); err != nil {
			return e.W(err, ECode070A08)
		}
	}

	// Flush any pending batch updates
	if err := sb.bu.Flush(); err != nil {
		return e.W(err, ECode070A09)
	}

	// Reset the list
	sb.list = nil

	return nil
}
