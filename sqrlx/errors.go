package sqrlx

import (
	"errors"

	"github.com/lib/pq"
)

// QueryError is thrown by all exec and query commands to wrap the driver error.
// It includes the statement causing the error
type QueryError struct {
	cause     error
	Statement string
}

// Cause gives the driver error which was thrown
func (err *QueryError) Unwrap() error {
	return err.cause
}

// Error is the cause error + the statement causing it
func (err *QueryError) Error() string {
	return err.cause.Error() + " `" + err.Statement + "` "
}

func (err *QueryError) AsPQError() (*pq.Error, bool) {
	var pqErr *pq.Error
	if errors.As(err.cause, &pqErr) {
		return pqErr, true
	}
	return nil, false
}

func AsPQError(err error) (*pq.Error, bool) {
	qe, ok := AsQueryError(err)
	if !ok {
		return nil, false
	}
	return qe.AsPQError()
}

func AsQueryError(err error) (*QueryError, bool) {
	var qe *QueryError
	ok := false

	if err == nil {
		return qe, false
	}

	ok = errors.As(err, &qe)
	return qe, ok
}

func AsConstraintViolationError(err error) (string, bool) {
	pqErr, ok := AsPQError(err)
	if !ok {
		return "", false
	}
	if pqErr.Code == "23505" {
		return pqErr.Constraint, true

	}
	return "", false
}

func IsConstraintViolationError(err error, constraint string) bool {
	cName, ok := AsConstraintViolationError(err)
	if !ok {
		return false
	}
	return cName == constraint
}
