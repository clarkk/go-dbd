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
		msg := operation+": "+err.Error()
		if ctx_canceled(err) {
			return nil, &Timeout_error{msg, errors.Wrap(err, 0).ErrorStack()}
		}
		panic(msg)
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
	
	return t.finish(operation, t.tx.Rollback())
}

func (t *Tx) Commit() error {
	const operation = "DB transaction commit"
	if t.tx == nil {
		panic(operation+": No active transaction")
	}
	
	if debug_log {
		t.log("COMMIT")
	}
	
	return t.finish(operation, t.tx.Commit())
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

func (t *Tx) finish(operation string, err error) error {
	t.tx = nil
	if err == nil {
		return nil
	}
	if err == sql.ErrTxDone {
		if ctx_err := t.ctx.Err(); ctx_err != nil {
			err = ctx_err
		}
	}
	msg := operation+": "+err.Error()
	if ctx_canceled(err) {
		return &Timeout_error{msg, errors.Wrap(err, 1).ErrorStack()}
	}
	panic(msg)
}

func (t *Tx) log(msg string){
	debug_log_sql(fmt.Sprintf("[TX #%d] %s", t.log_id, msg))
}