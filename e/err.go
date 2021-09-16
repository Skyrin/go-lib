package e

import (
	"fmt"

	"github.com/Skyrin/go-lib/errors"
)

// New creates a new error based on the code, id and message
func New(code, id, msg string) (err error) {
	return fmt.Errorf("%s%s: %s", code, id, msg)
}

// Contains checks if the error contains the code/id
func Contains(code, id string, err error) bool {
	return errors.ContainsError(err, fmt.Sprintf("%s%s: ", code, id))
}
