package dbd

import (
	"log"
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

func Is_empty_error(err error) bool {
	return errors.Is(err, sql.ErrNoRows)
}

func (e *Error) Error() string {
	return e.error
}

func (e *Error) Log(){
	log.Printf("%s: %s", e.error, e.stack)
}

func (e *Timeout_error) Error() string {
	return e.error
}

func ctx_canceled(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}