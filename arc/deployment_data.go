package arc

import (
	"encoding/json"
	"io/ioutil"
	"net/http"

	"github.com/Skyrin/go-lib/arc/model"
	"github.com/Skyrin/go-lib/arc/sqlmodel"
	"github.com/Skyrin/go-lib/sql"
)

// HTTPDataHandler handler for deployment data events
type HTTPDataHandler struct {
	Err error
	db  *sql.Connection
}

// err sets the error (so caller can see if there was one) then responds with
// an http error
func (hdh *HTTPDataHandler) err(w http.ResponseWriter, err error, msg string, code int) {
	hdh.Err = err
	http.Error(w, msg, code)
}

// NewHTTPDataHandler creates a new HTTP Data Handler
func NewHTTPDataHandler(db *sql.Connection) (adh *HTTPDataHandler) {
	return &HTTPDataHandler{
		db: db,
	}
}

// ServeHTTP handles the http request
func (hdh *HTTPDataHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		hdh.err(w, err, "invalid body", http.StatusBadRequest)
		return
	}

	d := &model.Data{}
	if err := json.Unmarshal([]byte(body), d); err != nil {
		hdh.err(w, err, err.Error(), http.StatusBadRequest)
		return
	}

	if !d.IsValidAppCode() {
		hdh.err(w, nil, "invalid app code specified", http.StatusBadRequest)
		return
	}

	if !d.IsValidType() {
		hdh.err(w, nil, "invalid type specified", http.StatusBadRequest)
		return
	}

	if err := sqlmodel.DataUpsert(hdh.db, d); err != nil {
		hdh.err(w, err, "failed to upsert record", http.StatusBadRequest)
		return
	}
}
