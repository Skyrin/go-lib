package arc

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/Skyrin/go-lib/arc/model"
	"github.com/Skyrin/go-lib/arc/sqlmodel"
	"github.com/Skyrin/go-lib/e"
	"github.com/Skyrin/go-lib/sql"
)

// DataHandler handler for deployment data events
type DataHandler struct {
	Err    error
	db     *sql.Connection
	client *Client

	// local cache for token authorization - might replace with db entry somewhere
	token       string
	tokenExpiry int64
}

// NewDataHandler creates a new Data Handler for saving arc data events into the arc_data table
func NewDataHandler(db *sql.Connection, client *Client) (adh *DataHandler) {

	return &DataHandler{
		db:     db,
		client: client,
	}
}

// auth authorizes the request by looking for a valid bearer token in the authorization
// header and either validating against the last cached token/expiry or the arc deployment
// If it successfully validates against the arc deployment, it will cache the token/expiry
// for future validations until the token expires or a new token is sent.
func (dh *DataHandler) auth(r *http.Request) (authorized bool, msg string) {
	// Verify the method is "POST"
	if r.Method != http.MethodPost {
		return false, "Must be a 'Post' request"
	}

	a := r.Header.Get("Authorization")
	if len(a) == 0 {
		return false, "'Authorization' header missing"
	}

	if !strings.HasPrefix(a, "Bearer ") {
		return false, "invalid 'Authorization' type, expecting bearer token"
	}

	t := a[7:]

	// Validate the token
	// TODO: store/lookup token/expiry in arc_config cache?
	cachedToken := dh.token
	cachedTokenExpiry := dh.tokenExpiry

	// If token matches and hasn't expired, then return true
	if cachedToken == t && cachedTokenExpiry > time.Now().Unix() {
		return true, ""
	}

	// Attempt to validate the token
	gui, err := dh.client.GrantUserinfo(t)
	if err != nil {
		// Going to ignore the error as it should just indicate the token
		// is invalid
		return false, e.MsgUnauthorized
	}

	// Ensure the type code is "app" as only the arc system can create an oauth2 token
	// for an "app" user
	if gui.TypeCode != "app" {
		// TODO: need further validation?
		return false, e.MsgUnauthorized
	}

	// Cache the token/expiry
	// TODO: save to db in something like arc_config table?
	dh.token = t
	tokenExpiry, err := strconv.Atoi(r.Header.Get("ArcTokenExpiry"))
	if err != nil {
		tokenExpiry = 0
	}
	dh.tokenExpiry = int64(tokenExpiry)

	return true, ""
}

// Publish attempts to save the published data. It will first authorize the call and if
// authorized, will save the data into the arc_data table. If it returns an error, then
// something bad happened and the calling method should log the error. If it returns
// anything but an empty message, then something was wrong with the call
func (dh *DataHandler) Publish(r *http.Request) (msg string, code int, err error) {

	authorized, msg := dh.auth(r)
	if !authorized {
		if msg == e.MsgUnauthorized {
			return e.MsgUnauthorized, http.StatusUnauthorized, nil
		}

		// If it isn't an unauthorized message, then it was a bad request
		return msg, http.StatusBadRequest, nil
	}

	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		return "invalid body", http.StatusBadRequest, nil
	}

	d := &model.Data{}
	if err := json.Unmarshal([]byte(body), d); err != nil {
		return err.Error(), http.StatusBadRequest, nil
	}

	if !d.IsValidAppCode() {
		return "invalid app code specified", http.StatusBadRequest, nil
	}

	if err := sqlmodel.DataUpsert(dh.db, d); err != nil {
		return e.MsgUnknownInternalServerError, http.StatusBadGateway,
			e.Wrap(err, e.Code0415, "01", fmt.Sprintf("data: %s", body))
	}

	return "", http.StatusOK, nil
}

// DataProcessor reads all pending records in the arc_data table and
// calls the app specific function to process it, then marks them all
// as processed.
type DataProcessor struct {
	db        *sql.Connection
	handle    func(*model.Data) error
	orderList []model.DataType
}

// NewDataProcess returns a new instance of a data processor
//  f: defines the function that will be called to handle each record
//  orderList: if specified, defines the order in which to fetch the records
//             based on the type
func NewDataProcessor(db *sql.Connection, f func(*model.Data) error,
	orderList []model.DataType) (dp *DataProcessor) {
	return &DataProcessor{
		db:        db,
		handle:    f,
		orderList: orderList,
	}
}

// Run iterates through all pending records in the arc_data table and calls
// the passed data handling function. Then marks all of the read records
// as processed.
func (dp *DataProcessor) Run() (err error) {
	txn, err := dp.db.BeginReturnDB()
	if err != nil {
		return e.Wrap(err, e.Code0416, "01")
	}
	// Mark all pending records as processing
	if err := sqlmodel.DataSetStatusProcessing(txn); err != nil {
		return e.Wrap(err, e.Code0416, "02")
	}

	// Iterate through all records in the processing status
	s := model.DataStatusProcessing
	_, _, err = sqlmodel.DataGet(txn, &sqlmodel.DataGetParam{
		Status:          &s,
		OrderByTypeList: dp.orderList,
		Handle:          dp.handle,
	})
	if err != nil {
		return e.Wrap(err, e.Code0416, "03")
	}

	// Mark all processing records as processed
	if err := sqlmodel.DataSetStatusProcessed(txn); err != nil {
		return e.Wrap(err, e.Code0416, "04")
	}

	if err := txn.Commit(); err != nil {
		return e.Wrap(err, e.Code0416, "05")
	}
	return nil
}

// DataProcessorSkip marks the arc_data record as pending (skipping so it will process again)
func DataProcessorSkip(db *sql.Connection, d *model.Data) (err error) {
	return sqlmodel.DataSetStatus(db, model.DataStatusPending, d)
}
