package http

import (
	"compress/gzip"
	"io"
	"net/http"
	"os"
	"strings"
)

const (
	// EnvAccessControlAllowOrigin for setting access control allow origin. Wildcard
	// (i.e. '*') is not allowed when using 'withCredentials', which is required
	// in order for the JS to use cookies
	EnvAccessControlAllowOrigin = "ACCESS_CONTROL_ALLOW_ORIGIN"
)

// NewServeMux returns a new http.ServeMux with routes supported by the API
func NewServeMux() (sMux *http.ServeMux, err error) {
	sMux = http.NewServeMux()

	return sMux, nil
}

// CORS add CORS headers to the response
func CORS(next http.Handler) http.Handler {
	// Get the access control allow origin
	accessControlAllowOrigin := os.Getenv(EnvAccessControlAllowOrigin)
	if accessControlAllowOrigin == "" {
		// If not set, then use the wildcard. Not, 'withCredentials' will
		// not work in JS with the wild card, meaning this default value
		// will not work with cookies
		accessControlAllowOrigin = "*"
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Remove the port if it is specified
		origin := r.Header.Get("Origin")
		if origin == "" {
			origin = accessControlAllowOrigin
		}
		w.Header().Add("Access-Control-Allow-Origin", origin)
		w.Header().Add("Access-Control-Allow-Credentials", "true")
		w.Header().Add("Access-Control-Allow-Headers", "Content-Type, X-Requested-With")
		w.Header().Add("Access-Control-Allow-Methods", "GET, POST")

		// Check the incoming Content-Type header and treat
		// text/plain as application/json
		if strings.Contains(r.Header.Get("Content-Type"), "text/plain") {
			r.Header.Set("Content-Type", "application/json")
		}

		next.ServeHTTP(w, r)
	})
}

type gzipResponseWriter struct {
	io.Writer
	http.ResponseWriter
}

func (w gzipResponseWriter) Write(b []byte) (int, error) {
	return w.Writer.Write(b)
}

// GZIP compress the response
func GZIP(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.Contains(r.Header.Get("Accept-Encoding"), "gzip") {
			next.ServeHTTP(w, r)
			return
		}
		w.Header().Set("Content-Encoding", "gzip")
		gz := gzip.NewWriter(w)
		defer gz.Close()
		gzr := gzipResponseWriter{Writer: gz, ResponseWriter: w}
		next.ServeHTTP(gzr, r)
	})
}
