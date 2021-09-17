package e

import (
	"errors"
	"fmt"
	"regexp"
	"strings"

	pkgerrors "github.com/pkg/errors"
)

// ExtendedError is our custom error
type ExtendedError struct {
	InnerError     error
	Message        string
	TruncateXLines int
	original       error
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

// New creates a new error based on the code, id and message, it also
// sets the Message property of the extended error to the passed message
func New(code, id, msg string) (err error) {
	return WrapWithMsg(nil, code, id, msg, msg)
}

// NewStr creates a new error string based on the code, id and message
func NewStr(code, id string, msgList ...string) (s string) {
	if len(msgList) == 0 {
		return fmt.Sprintf("%s%s", code, id)
	}
	return fmt.Sprintf("%s%s: %s", code, id, strings.Join(msgList, "|"))
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

// ContainsError checks if the error contains the specified error message
func ContainsError(err error, msg string) bool {
	return strings.Contains(err.Error(), msg)
}

// Contains checks if the error contains the code/id
func Contains(code, id string, err error) bool {
	return ContainsError(err, fmt.Sprintf("%s%s", code, id))
}

// WrapWithMessage calls Wrap, then sets the extended error's message to
// the passed message.
func WrapWithMsg(err error, code, id, msg string, debugMessages ...string) error {
	ee := Wrap(err, code, id, debugMessages...)
	ee.Message = NewStr(code, id, msg)
	return ee
}

// Wrap checks if the passed error has been wrapped before by this func
// and either wraps the original error as an ExtendedError or adds the
// debug message to the already existing ExtendedError's InnerError. It
// will also overwrite the current ExtendedError's user message if the
// passed userMsg is not empty
// i.e. is it an ExtendedError. If not, it will create an ExtendedError,
// assign the InnerError and UserMsg to it and then return it. If it already
// is an ExtendedError
// This function always returns an extended error, but the signature is
// error
func Wrap(err error, code, id string, debugMessages ...string) (ee *ExtendedError) {
	msg := NewStr(code, id, debugMessages...)

	// If the error is already an extended error, then just update the
	// inner error
	if ee = AsExtendedError(err); ee != nil {
		ee.InnerError = fmt.Errorf("[%s]%+v", msg, ee)
		return ee
	}

	ee = &ExtendedError{
		// UserMsg:  userMsg,
		original: err,
	}

	if err == nil {
		ee.InnerError = pkgerrors.New(msg)
		ee.Message = msg
	} else {
		var pkgerr error
		if IsPQError(err, PQErr58030IOError) {
			msg = editErrorMessageForPQIOError(err.Error())
			pkgerr = pkgerrors.Wrap(err, msg)
		} else {
			pkgerr = pkgerrors.Wrap(err, "")
		}

		ee.InnerError = fmt.Errorf("[%s]%+v", msg, pkgerr)
		ee.Message = NewStr(code, id, MsgUnknownInternalServerError)
	}

	return ee
}
