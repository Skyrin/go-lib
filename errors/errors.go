package errors

import (
	"errors"
	"fmt"

	pkgerrors "github.com/pkg/errors"
)

var CustomExtendedError *ExtendedError = &ExtendedError{}

type ExtendedError struct {
	InnerError error
	UserMsg    string
	original error
}

// Error returns the string of the inner error
func (e *ExtendedError) Error() string {
	return fmt.Sprintf("%+v", e.InnerError)
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
		UserMsg: userMsg,
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
