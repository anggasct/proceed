package store

import (
	"errors"
	"fmt"
)

const (
	CodeGraphInvalid  = "GRAPH_INVALID"
	CodeStoreBusy     = "STORE_BUSY"
	CodeStoreConflict = "STORE_CONFLICT"
)

type Error struct {
	Code string
	Msg  string
}

func (e *Error) Error() string {
	return e.Code + ": " + e.Msg
}

func storeErr(code, format string, args ...any) *Error {
	return &Error{Code: code, Msg: fmt.Sprintf(format, args...)}
}

func NewCodeError(code, format string, args ...any) *Error {
	return storeErr(code, format, args...)
}

func AsStoreError(err error) (*Error, bool) {
	var e *Error
	if errors.As(err, &e) {
		return e, true
	}
	return nil, false
}

func ErrorCode(err error) string {
	if e, ok := AsStoreError(err); ok {
		return e.Code
	}
	return ""
}

func IsCode(err error, code string) bool {
	return ErrorCode(err) == code
}
