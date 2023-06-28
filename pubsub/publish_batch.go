package pubsub

import (
	"strconv"
	"strings"

	"github.com/Skyrin/go-lib/e"
	"github.com/Skyrin/go-lib/pubsub/internal/sqlmodel"
	"github.com/Skyrin/go-lib/pubsub/model"
	"github.com/Skyrin/go-lib/sql"
)

const (
	// Error constants
	ECode070D01 = e.Code070D + "01"
	ECode070D02 = e.Code070D + "02"
	ECode070D03 = e.Code070D + "03"
	ECode070D04 = e.Code070D + "04"
	ECode070D05 = e.Code070D + "05"
)

// BatchPublisher helper to publish records in batches. Initialize and set the
// batch size. Then call Add, and records will be automatically committed when
// the batch size is reached. Call Flush to commit any pending records and Close
// when finished.
type BatchPublisher struct {
	bi   *sqlmodel.DataBulkInsert
	list map[string]*model.Data
}

// BatchPublisherParam params sent to NewBatchPublisher
type BatchPublisherParam struct {
	BatchSize  uint                           // The size of each batch
	PreCommit  func() error                   // Called right before a commit of records
	PostCommit func(committedCount int) error // Called right after a commit of records
}

// NewBatchPublisher creates a new batch publisher to upserts pub data record
// in batches.
func NewBatchPublisher(db *sql.Connection, p *BatchPublisherParam) (bp *BatchPublisher) {
	bp = &BatchPublisher{
		bi:   sqlmodel.NewDataBulkInsert(db),
		list: make(map[string]*model.Data),
	}

	bp.bi.SetPreInsert(p.PreCommit)
	bp.bi.SetPostInsert(p.PostCommit)
	bp.bi.SetMaxRowPerInsert(p.BatchSize)

	return bp
}

// SetPreCommit sets the pre commit func called right before each commit. Set to
// nil to disable
func (bp *BatchPublisher) SetPreCommit(f func() error) {
	bp.bi.SetPreInsert(f)
}

// SetPostCommit sets the post commit func called right after each commit. Set
// to nil to disable
func (bp *BatchPublisher) SetPostCommit(f func(commitCount int) error) {
	bp.bi.SetPostInsert(f)
}

// Close closes any open database statements. This should be called when finished
// with the batch publishing.
func (bp *BatchPublisher) Close() (errList []error) {
	return bp.bi.Close()
}

// SetBatchSize set the batch size. The actual batch size may be reduced if it
// exceeds the maximum allowed rows per insert based on the number of columns in
// the bulk insert.
func (bp *BatchPublisher) SetBatchSize(size uint) (actualBatchSize uint) {
	return bp.bi.SetMaxRowPerInsert(size)
}

// GetBatchSize get the currently set batch size.
func (bp *BatchPublisher) GetBatchSize() (batchSize uint) {
	return bp.bi.GetMaxRowPerInsert()
}

// Add adds the record to the pending publish list. If the size of the list
// exceeds the batch size, then it will automatically commit the pending records.
func (bp *BatchPublisher) Add(pp PublishParam) (commitCount int, err error) {
	if pp.Type == "" {
		return 0, e.N(ECode070D01, "missing type")
	}
	if pp.ID == "" {
		return 0, e.N(ECode070D02, "missing id")
	}
	id := strings.Join([]string{strconv.Itoa(pp.PublishID), pp.Type, pp.ID}, ".")

	if bp.list[id] != nil {
		// Already in the list, skip it
		return 0, e.N(ECode070D03, "duplicate pending record")
	}

	d := &model.Data{
		PubID:   pp.PublishID,
		Type:    pp.Type,
		ID:      pp.ID,
		Deleted: pp.Deleted,
		JSON:    pp.JSON,
	}
	bp.list[id] = d
	rowsUpdated, err := bp.bi.Add(d)
	if err != nil {
		return 0, e.W(err, ECode070D04)
	}
	if rowsUpdated > 0 {
		// Reset list since records were saved
		bp.list = make(map[string]*model.Data)
	}

	return rowsUpdated, nil
}

// Flush commits any pending records.
func (bp *BatchPublisher) Flush() (commitCount int, err error) {
	rowsUpdated := bp.bi.GetCount()
	if err := bp.bi.Flush(); err != nil {
		return 0, e.W(err, ECode070D04)
	}

	// Reset list
	bp.list = make(map[string]*model.Data)

	return rowsUpdated, nil
}

// GetList get the current list  of records
func (bp *BatchPublisher) GetList() (mList []*model.Data) {
	mList = make([]*model.Data, len(bp.list))

	i := 0
	for _, d := range bp.list {
		mList[i] = d
		i++
	}

	return mList
}
