package dbd

import (
	"fmt"
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
			return nil, &Timeout_error{fmt.Errorf("DB transaction begin: %w", err), errors.Wrap(err, 0).ErrorStack()}
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
			return &Timeout_error{fmt.Errorf("DB transaction rollback: %w", err), errors.Wrap(err, 0).ErrorStack()}
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
			return &Timeout_error{fmt.Errorf("DB transaction commit: %w", err), errors.Wrap(err, 0).ErrorStack()}
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
		return &Error{fmt.Errorf("DB transaction query row compile: %w", err), errors.Wrap(err, 0).ErrorStack()}
	}
	
	if err := t.tx.QueryRowContext(t.ctx, sql, query.Data()...).Scan(scan...); err != nil {
		if Is_empty_error(err) {
			return err
		}
		werr := fmt.Errorf("DB transaction query row: %w", err)
		stack := errors.Wrap(err, 0).ErrorStack()
		if ctx_canceled(err) {
			return &Timeout_error{werr, stack}
		}
		return &Error{werr, stack}
	}
	return nil
}

func (t *Tx) Query(query sqlc.SQL) (*sql.Rows, error){
	if t.tx == nil {
		panic("DB transaction query: No active transaction")
	}
	
	sql, err := query.Compile()
	if err != nil {
		return nil, &Error{fmt.Errorf("DB transaction query compile: %w", err), errors.Wrap(err, 0).ErrorStack()}
	}
	
	rows, err := t.tx.QueryContext(t.ctx, sql, query.Data()...)
	if err != nil {
		werr := fmt.Errorf("DB transaction query: %w", err)
		stack := errors.Wrap(err, 0).ErrorStack()
		if ctx_canceled(err) {
			return nil, &Timeout_error{werr, stack}
		}
		return nil, &Error{werr, stack}
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
		return id, &Error{fmt.Errorf("DB transaction insert compile: %w", err), errors.Wrap(err, 0).ErrorStack()}
	}
	
	if err := t.tx.QueryRowContext(t.ctx, sql+" RETURNING id", query.Data()...).Scan(&id); err != nil {
		werr := fmt.Errorf("DB transaction insert: %w", err)
		stack := errors.Wrap(err, 0).ErrorStack()
		if ctx_canceled(err) {
			return 0, &Timeout_error{werr, stack}
		}
		return 0, &Error{werr, stack}
	}
	return id, nil
}

func (t *Tx) Update(query sqlc.SQL) error {
	if t.tx == nil {
		panic("DB transaction update: No active transaction")
	}
	
	sql, err := query.Compile()
	if err != nil {
		return &Error{fmt.Errorf("DB transaction update compile: %w", err), errors.Wrap(err, 0).ErrorStack()}
	}
	
	if _, err := t.tx.ExecContext(t.ctx, sql, query.Data()...); err != nil {
		werr := fmt.Errorf("DB transaction update: %w", err)
		stack := errors.Wrap(err, 0).ErrorStack()
		if ctx_canceled(err) {
			return &Timeout_error{werr, stack}
		}
		return &Error{werr, stack}
	}
	return nil
}