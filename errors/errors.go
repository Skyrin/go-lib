package errors

import (
	"errors"
	"fmt"
	"regexp"

	pkgerrors "github.com/pkg/errors"
)

// CustomExtendedError is the custom error object
var CustomExtendedError *ExtendedError = &ExtendedError{}

// ExtendedError is our custom error
type ExtendedError struct {
	InnerError     error  `json:"innerError"`
	UserMsg        string `json:"userMsg"`
	original       error
	TruncateXLines int
}

// Error returns the string of the inner error
func (e *ExtendedError) Error() string {
	s := fmt.Sprintf("%+v", e.InnerError)
	if e.TruncateXLines == 0 {
		return s
	}

	// TODO: implement differently? this is really just for local dev
	// Truncate the last x lines
	r := []rune(s)
	idx, numNewLines := 0, 0

	for i := len(s) - 1; i > 0 && numNewLines < e.TruncateXLines; i-- {
		if r[i] == '\n' {
			idx = i
			numNewLines++
		}
	}

	if numNewLines > 0 {
		r = r[0:idx]
	}

	return string(r)
}

// IsError checks if the originating error is the specified target
func (e *ExtendedError) IsError(tgt error) bool {
	return errors.Is(e.original, tgt)
}

// AsError calls errors.As on the original error with the specified target error.
// If it is the target error, it will set the target as the original error value
// and return true, otherwise it returns false
func (e *ExtendedError) AsError(tgt interface{}) bool {
	return errors.As(e.original, tgt)
}

// Wrap checks if the passed error has been wrapped before by this func
// and either wraps the original error as an ExtendedError or adds the
// debug message to the already existing ExtendedError's InnerError. It
// will also overwrite the current ExtendedError's user message if the
// passed userMsg is not empty
// i.e. is it an ExtendedError. If not, it will create an ExtendedError,
// assign the InnerError and UserMsg to it and then return it. If it already
// is an ExtendedError
func Wrap(err error, debugMsg, userMsg string) error {
	if ee := AsExtendedError(err); ee != nil {
		if userMsg != "" {
			ee.UserMsg = userMsg
		}
		ee.InnerError = fmt.Errorf("[%s]%+v", debugMsg, ee)
		return ee
	}
	ee := &ExtendedError{
		UserMsg:  userMsg,
		original: err,
	}
	if err == nil {
		// If no user message is set, then set to unknown internal server error
		// This can get overwritten later if needed
		if ee.UserMsg == "" {
			ee.UserMsg = UnknownInternalServerError
		}
		ee.InnerError = pkgerrors.New(debugMsg)
	} else {
		if IsPQError(err, PQErr58030IOError) {
			debugMsg = editErrorMessageForPQIOError(err.Error())
		}

		ee.InnerError = pkgerrors.Wrap(err, debugMsg)
	}

	return ee
}

// AsExtendedError helper function that returns the error as an ExtendedError
// if it is one. Otherwise it returns nil
func AsExtendedError(err error) (ee *ExtendedError) {
	if errors.As(err, &ee) {
		return ee
	}
	return nil
}

// editErrorMessageForPQIOError returns an edited message for the error if it is a pg io error
// this is so the logs do not consider it like a new error if it is triggered within a short period of time
func editErrorMessageForPQIOError(errorMsg string) string {
	re := regexp.MustCompile(`block [\d]+`)

	return re.ReplaceAllString(errorMsg, "block X")
}

// NewErr returns an error with the friendly user message from an ExtendedError, so it doesn't get logged
func NewErr(err error) error {
	return fmt.Errorf(AsExtendedError(err).UserMsg)
}
