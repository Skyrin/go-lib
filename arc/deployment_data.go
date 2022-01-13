package arc

import (
	"encoding/json"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/Skyrin/go-lib/arc/model"
	"github.com/Skyrin/go-lib/arc/sqlmodel"
	"github.com/Skyrin/go-lib/sql"
	"github.com/rs/zerolog/log"
)

// HTTPDataHandler handler for deployment data events
type HTTPDataHandler struct {
	Err          error
	db           *sql.Connection
	client       *Client
	credentialID int
	// loginURI     string // The path that will handle login
	publishURI string // The path that will accept published data

	// Temp for testing
	token       string
	tokenExpiry int64
}

// err sets the error (so caller can see if there was one) then responds with
// an http error
func (hdh *HTTPDataHandler) err(w http.ResponseWriter, err error, msg string, code int) {
	hdh.Err = err
	http.Error(w, msg, code)
}

// NewHTTPDataHandler creates a new HTTP Data Handler
// The publishURI defines the path that will accept published arc data and save it
// for processing into the application
func NewHTTPDataHandler(db *sql.Connection, client *Client,
	credentialID int, publishURI string) (adh *HTTPDataHandler) {

	return &HTTPDataHandler{
		db:           db,
		client:       client,
		credentialID: credentialID,
		publishURI:   publishURI,
	}
}

// ServeHTTP handles the http request
func (hdh *HTTPDataHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Path {
	case hdh.publishURI:
		hdh.handlePublish(w, r)
	default:
		hdh.err(w, nil, "URI not recognized", http.StatusBadRequest)
	}
}

// auth authorizes the request, looking for a valid bearer token
func (hdh *HTTPDataHandler) auth(w http.ResponseWriter, r *http.Request) (
	authorized bool) {

	a := r.Header.Get("Authorization")
	if len(a) == 0 {
		hdh.err(w, nil, "'Authorization' header missing", http.StatusBadRequest)
		return false
	}

	if !strings.HasPrefix(a, "Bearer ") {
		hdh.err(w, nil, "invalid 'Authorization' type, expecting bearer token", http.StatusBadRequest)
		return false
	}

	t := a[7:]

	// Validate the token
	// TODO: lookup token/expiry in arc_config cache
	cachedToken := hdh.token
	cachedTokenExpiry := hdh.tokenExpiry

	// If token matches and hasn't expired, then return true
	if cachedToken == t && cachedTokenExpiry < time.Now().Unix() {
		return true
	}

	// Attempt to validate the token
	gui, err := hdh.client.GrantUserinfo(t)
	if err != nil {
		hdh.err(w, nil, "Unauthorized", http.StatusUnauthorized)
		// Going to ignore the error as it should just indicate the token
		// is invalid
		return false
	}

	if gui.TypeCode != "app" {
		hdh.err(w, nil, "Unauthorized", http.StatusUnauthorized)
		// TODO: need to validate the user?
		return false
	}

	// Cache the token/expiry
	// TODO: save to arc_config
	hdh.token = t
	tokenExpiry, err := strconv.Atoi(r.Header.Get("ArcTokenExpiry"))
	if err != nil {
		tokenExpiry = 0
	}
	hdh.tokenExpiry = int64(tokenExpiry)

	return true
}

func (hdh *HTTPDataHandler) handlePublish(w http.ResponseWriter, r *http.Request) {
	// Authorize the request
	if !hdh.auth(w, r) {
		return
	}

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
