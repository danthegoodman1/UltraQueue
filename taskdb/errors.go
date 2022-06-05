package taskdb

import "errors"

var (
	ErrPayloadNotFound = errors.New("payload not found")
	ErrStateNotFound   = errors.New("state not found")
)
