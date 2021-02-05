package lib

import (
	"fmt"

	"github.com/pkg/errors"
)

var CustomExtendedError *ExtendedError = &ExtendedError{}

type ExtendedError struct {
	InnerError error
	UserMsg    string
	ShouldLog  bool
}

func (e *ExtendedError) Error() string {
	return fmt.Sprintf("%+v", e.InnerError)
}

func (e *ExtendedError) Is(tgt error) bool {
	_, ok := tgt.(*ExtendedError)

	return ok
}

func NewCError(err error, debugMsgKey, userMsgKey string, log bool) (customError *ExtendedError) {
	var errNew error
	if err == nil {
		errNew = errors.New(debugMsgKey)
	} else {
		errNew = errors.Wrap(err, debugMsgKey)
	}
	return &ExtendedError{
		InnerError: errNew,
		UserMsg:    userMsgKey,
		ShouldLog:  log,
	}
}
