package dbd

import (
	"errors"
	"context"
	"database/sql"
)

type (
	Error struct {
		error 	string
		err 	error
		stack 	string
	}
	
	Timeout_error struct {
		error 	string
		err 	error
	}
)

func Is_empty_error(err error) bool {
	return errors.Is(err, sql.ErrNoRows)
}

func As_error(err error) (terr *Error){
	if errors.As(err, &terr) {
		return terr
	}
	return nil
}

/*func As_timeout_error(err error) (terr *Timeout_error){
	if errors.As(err, &terr) {
		return terr
	}
	return nil
}*/

func (e *Error) Error() string {
	return e.error
}

func (e *Error) Unwrap() error {
	return e.err
}

func (e *Error) Stack() string {
	return e.stack
}

func (e *Timeout_error) Error() string {
	return e.error
}

func (e *Timeout_error) Unwrap() error {
	return e.err
}

func ctx_canceled(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}