package adapter

import (
	"errors"
	"fmt"
)

type ErrorKind string

const (
	ErrorKindValidation ErrorKind = "validation"
	ErrorKindNormalize  ErrorKind = "normalize"
	ErrorKindPublish    ErrorKind = "publish"
	ErrorKindCheckpoint ErrorKind = "checkpoint"
	ErrorKindSource     ErrorKind = "source"
)

type ApplicationError struct {
	Kind      ErrorKind
	Operation string
	Retryable bool
	Message   string
	Cause     error
	Cursor    int64
	EventId   string
	Key       string
	SourceId  string
	NodeId    string
}

func (e *ApplicationError) Error() string {
	if e.Cause == nil {
		return fmt.Sprintf("%s: %s", e.Operation, e.Message)
	}
	return fmt.Sprintf("%s: %s: %v", e.Operation, e.Message, e.Cause)
}

func (e *ApplicationError) Unwrap() error { return e.Cause }

func IsRetryable(err error) bool {
	var ae *ApplicationError
	if errors.As(err, &ae) {
		return ae.Retryable
	}
	return false
}
