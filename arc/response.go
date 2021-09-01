package arc

import (
	"encoding/json"
	"fmt"

	"github.com/Skyrin/go-lib/errors"
)

// ResponseList represents the notification service response
type ResponseList struct {
	ID        int        `json:"id"`
	Success   bool       `json:"success"`
	Responses []Response `json:"responses"`
	Message   string     `json:"message"`
	Format    string     `json:"format"`
	Code      int        `json:"code"`
}

// Response represents the response from Arc
type Response struct {
	ID        int             `json:"id"`
	Success   bool            `json:"success"`
	Code      int             `json:"code"`
	ErrorCode string          `json:"errorCode"`
	Message   string          `json:"message"`
	Data      json.RawMessage `json:"data"`
	Errors    []string        `json:"errors"`
}

// responseErrors returns errors found in the response if any.  Can add other checks for errors
func (nrl *ResponseList) responseErrors() error {
	if !nrl.Success {
		return errors.Wrap(fmt.Errorf("%+v", nrl), "responseErrors.1", "")
	}

	// Let the caller check success of individual responses
	// for _, v := range nrl.Responses {
	// 	if !v.Success {
	// 		return errors.Wrap(fmt.Errorf("%+v", nrl), "responseErrors.2", "")
	// 	}
	// }

	return nil
}
