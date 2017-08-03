package tnt

import "errors"

var (
	// ErrRequestTimeout means timeout while sending request.
	ErrRequestTimeout = NewConnectionError("Request send timeout")
	// ErrResponseTimeout means timeout while request waiting.
	ErrResponseTimeout = NewConnectionError("Response read timeout")
	// ErrConnectionClosed means connection have been closed already.
	ErrConnectionClosed = NewConnectionError("Connection closed")
	// ErrShredOldRequests means request ID error.
	ErrShredOldRequests = NewConnectionError("Shred old requests")
)

type ConnectionError struct {
	error
}

type QueryError struct {
	error
}

func NewConnectionError(message string) error {
	return &ConnectionError{
		error: errors.New(message),
	}
}

func NewQueryError(message string) error {
	return &QueryError{
		error: errors.New(message),
	}
}
