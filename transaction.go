package dbd

import (
	"fmt"
	"context"
	"sync/atomic"
	"database/sql"
	"github.com/go-errors/errors"
	"github.com/clarkk/go-dbd/sqlc"
)

var log_id uint64

type Tx struct {
	ctx		context.Context
	tx		*sql.Tx
	log_id	uint64
}

func NewTx(ctx context.Context) (*Tx, error){
	const operation = "DB transaction begin"
	tx := &Tx{
		ctx: ctx,
	}
	
	if debug_log {
		tx.log_id = atomic.AddUint64(&log_id, 1)
		tx.log("BEGIN")
	}
	
	var err error
	if tx.tx, err = db.BeginTx(ctx, nil); err != nil {
		if ctx_canceled(err) {
			return nil, &Timeout_error{operation+": "+err.Error(), errors.Wrap(err, 0).ErrorStack()}
		}
		panic(operation+": "+err.Error())
	}
	return tx, nil
}

func (t *Tx) Context() context.Context {
	return t.ctx
}

func (t *Tx) Rollback() error {
	const operation = "DB transaction rollback"
	if t.tx == nil {
		return nil
	}
	
	if debug_log {
		t.log("ROLLBACK")
	}
	
	if err := t.tx.Rollback(); err != nil {
		t.tx = nil
		if ctx_canceled(err) {
			return &Timeout_error{operation+": "+err.Error(), errors.Wrap(err, 0).ErrorStack()}
		}
		return &Error{operation+": "+err.Error(), errors.Wrap(err, 0).ErrorStack()}
	}
	t.tx = nil
	return nil
}

func (t *Tx) Commit() error {
	const operation = "DB transaction commit"
	if t.tx == nil {
		panic(operation+": No active transaction")
	}
	
	if debug_log {
		t.log("COMMIT")
	}
	
	if err := t.tx.Commit(); err != nil {
		t.tx = nil
		if ctx_canceled(err) {
			return &Timeout_error{operation+": "+err.Error(), errors.Wrap(err, 0).ErrorStack()}
		}
		panic(operation+": "+err.Error())
	}
	t.tx = nil
	return nil
}

func (t *Tx) Exec(query sqlc.SQL) (sql.Result, error){
	const operation = "DB transaction execute"
	if t.tx == nil {
		panic(operation+": No active transaction")
	}
	return exec(t.ctx, query, t.tx.ExecContext, t.log, operation)
}

func (t *Tx) Query_row(query sqlc.SQL, scan []any) (empty bool, err error){
	const operation = "DB transaction query row"
	if t.tx == nil {
		panic(operation+": No active transaction")
	}
	return query_row(t.ctx, query, scan, t.tx.QueryRowContext, t.log, operation)
}

func (t *Tx) Query(query sqlc.SQL) (*sql.Rows, error){
	const operation = "DB transaction query"
	if t.tx == nil {
		panic(operation+": No active transaction")
	}
	return query_rows(t.ctx, query, t.tx.QueryContext, t.log, operation)
}

func (t *Tx) Insert(query sqlc.SQL) (uint64, error){
	const operation = "DB transaction insert"
	if t.tx == nil {
		panic(operation+": No active transaction")
	}
	return insert(t.ctx, query, t.tx.QueryRowContext, t.log, operation)
}

func (t *Tx) Insert_no_return(query sqlc.SQL) error {
	const operation = "DB transaction insert no return"
	if t.tx == nil {
		panic(operation+": No active transaction")
	}
	_, err := exec(t.ctx, query, t.tx.ExecContext, t.log, operation)
	return err
}

func (t *Tx) Update(query sqlc.SQL) error {
	const operation = "DB transaction update"
	if t.tx == nil {
		panic(operation+": No active transaction")
	}
	_, err := exec(t.ctx, query, t.tx.ExecContext, t.log, operation)
	return err
}

func (t *Tx) Delete(query sqlc.SQL) (bool, error){
	const operation = "DB transaction delete"
	if t.tx == nil {
		panic(operation+": No active transaction")
	}
	return delete(t.ctx, query, t.tx.QueryRowContext, t.log, operation)
}

func (t *Tx) log(msg string){
	debug_log_sql(fmt.Sprintf("[TX #%d] %s", t.log_id, msg))
}