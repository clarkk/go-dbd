package dbd

import (
	"errors"
	"context"
	"database/sql"
)

var ErrNotFound = errors.New("Not found")

type (
	Error struct {
		error 	string
		stack 	string
	}
	
	Timeout_error struct {
		error 	string
		stack 	string
	}
)

func No_rows_error(err error) bool {
	return errors.Is(err, sql.ErrNoRows)
}

func (e *Error) Error() string {
	return e.error+"\n"+e.stack
}

func (e *Timeout_error) Error() string {
	return e.error
}

func ctx_canceled(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}