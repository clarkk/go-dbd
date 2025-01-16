package dbd

import (
	"context"
	"database/sql"
	"github.com/go-errors/errors"
	"github.com/clarkk/go-dbd/sqlc"
)

type Tx struct {
	ctx		context.Context
	tx		*sql.Tx
}

func NewTx(ctx context.Context) (*Tx, error){
	tx := &Tx{
		ctx: ctx,
	}
	var err error
	if tx.tx, err = db.BeginTx(ctx, nil); err != nil {
		if ctx_canceled(err) {
			return nil, &Timeout_error{"DB transaction begin: "+err.Error(), errors.Wrap(err, 0).ErrorStack()}
		}
		panic("DB transaction begin: "+err.Error())
	}
	return tx, nil
}

func (t *Tx) Rollback() error {
	if t.tx == nil {
		return nil
	}
	if err := t.tx.Rollback(); err != nil {
		t.tx = nil
		if ctx_canceled(err) {
			return &Timeout_error{"DB transaction rollback: "+err.Error(), errors.Wrap(err, 0).ErrorStack()}
		}
		panic("DB transaction rollback: "+err.Error())
	}
	t.tx = nil
	return nil
}

func (t *Tx) Commit() error {
	if t.tx == nil {
		panic("DB transaction commit: No active transaction")
	}
	if err := t.tx.Commit(); err != nil {
		t.tx = nil
		if ctx_canceled(err) {
			return &Timeout_error{"DB transaction commit: "+err.Error(), errors.Wrap(err, 0).ErrorStack()}
		}
		panic("DB transaction commit: "+err.Error())
	}
	t.tx = nil
	return nil
}

func (t *Tx) Query_row(query sqlc.SQL, scan []any) error {
	if t.tx == nil {
		panic("DB transaction query row: No active transaction")
	}
	
	sql, err := query.Compile()
	if err != nil {
		return &Error{"DB transaction query row compile: "+err.Error(), errors.Wrap(err, 0).ErrorStack()}
	}
	
	if err := t.tx.QueryRowContext(t.ctx, sql, query.Data()...).Scan(scan...); err != nil {
		if Is_empty_error(err) {
			return err
		}
		msg := "DB transaction query row: "+err.Error()
		stack := errors.Wrap(err, 0).ErrorStack()
		if ctx_canceled(err) {
			return &Timeout_error{msg, stack}
		}
		return &Error{msg, stack}
	}
	return nil
}

func (t *Tx) Query(query sqlc.SQL) (*sql.Rows, error){
	if t.tx == nil {
		panic("DB transaction query: No active transaction")
	}
	
	sql, err := query.Compile()
	if err != nil {
		return nil, &Error{"DB transaction query compile: "+err.Error(), errors.Wrap(err, 0).ErrorStack()}
	}
	
	rows, err := t.tx.QueryContext(t.ctx, sql, query.Data()...)
	if err != nil {
		msg := "DB transaction query: "+err.Error()
		stack := errors.Wrap(err, 0).ErrorStack()
		if ctx_canceled(err) {
			return nil, &Timeout_error{msg, stack}
		}
		return nil, &Error{msg, stack}
	}
	return rows, nil
}

func (t *Tx) Insert(query sqlc.SQL) (int, error){
	if t.tx == nil {
		panic("DB transaction insert: No active transaction")
	}
	
	var id int
	sql, err := query.Compile()
	if err != nil {
		return id, &Error{"DB transaction insert compile: "+err.Error(), errors.Wrap(err, 0).ErrorStack()}
	}
	
	if err := t.tx.QueryRowContext(t.ctx, sql+" RETURNING id", query.Data()...).Scan(&id); err != nil {
		msg := "DB transaction insert: "+err.Error()
		stack := errors.Wrap(err, 0).ErrorStack()
		if ctx_canceled(err) {
			return 0, &Timeout_error{msg, stack}
		}
		return 0, &Error{msg, stack}
	}
	return id, nil
}

func (t *Tx) Update(query sqlc.SQL) error {
	if t.tx == nil {
		panic("DB transaction update: No active transaction")
	}
	
	sql, err := query.Compile()
	if err != nil {
		return &Error{"DB transaction update compile: "+err.Error(), errors.Wrap(err, 0).ErrorStack()}
	}
	
	if _, err := t.tx.ExecContext(t.ctx, sql, query.Data()...); err != nil {
		msg := "DB transaction update: "+err.Error()
		stack := errors.Wrap(err, 0).ErrorStack()
		if ctx_canceled(err) {
			return &Timeout_error{msg, stack}
		}
		return &Error{msg, stack}
	}
	return nil
}