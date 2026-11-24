package dbd

import (
	"fmt"
	"time"
	"runtime"
	"context"
	"database/sql"
	"github.com/go-errors/errors"
	_ "github.com/go-sql-driver/mysql"
	"github.com/clarkk/go-dbd/sqlc"
)

var db *sql.DB

type (
	exec_func		func(ctx context.Context, sql string, data ...any) (sql.Result, error)
	query_row_func	func(ctx context.Context, sql string, data ...any) *sql.Row
	query_func		func(ctx context.Context, sql string, data ...any) (*sql.Rows, error)
	
	log_func		func(msg string)
)

func Connect(dsn string, conn_cpu int) error {
	if db != nil {
		return fmt.Errorf("dbd: Database already open")
	}
	if conn_cpu <= 0 {
		return fmt.Errorf("dbd: conn_cpu must be positive")
	}
	
	var err error
	db, err = sql.Open("mysql", dsn)
	if err != nil {
		return fmt.Errorf("dbd: Open: %w", err)
	}
	
	db.SetMaxOpenConns(runtime.NumCPU() * conn_cpu)
	db.SetMaxIdleConns(10)
	db.SetConnMaxLifetime(30 * time.Minute)
	db.SetConnMaxIdleTime(5 * time.Minute)
	
	if err := Ping(); err != nil {
		Close()
		return fmt.Errorf("dbd: ping: %w", err)
	}
	
	return nil
}

func Ping() error {
	return db.Ping()
}

func Exec(ctx context.Context, query sqlc.SQL) (sql.Result, error){
	return exec(ctx, query, db.ExecContext, debug_log_sql, "DB execute")
}

func Query_row(ctx context.Context, query sqlc.SQL, scan []any) (empty bool, err error){
	return query_row(ctx, query, scan, db.QueryRowContext, debug_log_sql, "DB query row")
}

func Query(ctx context.Context, query sqlc.SQL) (*sql.Rows, error){
	return query_rows(ctx, query, db.QueryContext, debug_log_sql, "DB query")
}

func Insert(ctx context.Context, query sqlc.SQL) (uint64, error){
	return insert(ctx, query, db.QueryRowContext, debug_log_sql, "DB insert")
}

func Update(ctx context.Context, query sqlc.SQL) (sql.Result, error){
	return exec(ctx, query, db.ExecContext, debug_log_sql, "DB update")
}

func Delete(ctx context.Context, query sqlc.SQL) (bool, error){
	return delete(ctx, query, db.QueryRowContext, debug_log_sql, "DB delete")
}

func Close() error {
	if db == nil {
		return nil
	}
	err := db.Close()
	db = nil
	return err
}

func exec(ctx context.Context, query sqlc.SQL, run exec_func, log_sql log_func, operation string) (sql.Result, error){
	sql, data, err := query.Compile()
	if err != nil {
		return nil, compile_error(operation, err)
	}
	
	if debug_log {
		log_sql(sqlc.SQL_debug(query))
	}
	
	result, err := run(ctx, sql, data...)
	if err != nil {
		return nil, query_error(operation, query, err)
	}
	return result, nil
}

func query_row(ctx context.Context, query sqlc.SQL, scan []any, run query_row_func, log_sql log_func, operation string) (empty bool, err error){
	sql, data, err := query.Compile()
	if err != nil {
		return false, compile_error(operation, err)
	}
	
	if debug_log {
		log_sql(sqlc.SQL_debug(query))
	}
	
	if err = run(ctx, sql, data...).Scan(scan...); err == nil {
		return false, nil
	}
	if No_rows_error(err) {
		return true, ErrNotFound
	}
	return false, query_error(operation, query, err)
}

func query_rows(ctx context.Context, query sqlc.SQL, run query_func, log_sql log_func, operation string) (*sql.Rows, error){
	sql, data, err := query.Compile()
	if err != nil {
		return nil, compile_error(operation, err)
	}
	
	if debug_log {
		log_sql(sqlc.SQL_debug(query))
	}
	
	rows, err := run(ctx, sql, data...)
	if err != nil {
		return nil, query_error(operation, query, err)
	}
	return rows, nil
}

func insert(ctx context.Context, query sqlc.SQL, run query_row_func, log_sql log_func, operation string) (uint64, error){
	sql, data, err := query.Compile()
	if err != nil {
		return 0, compile_error(operation, err)
	}
	
	if debug_log {
		log_sql(sqlc.SQL_debug(query))
	}
	
	var id uint64
	if err := run(ctx, sql+" RETURNING id", data...).Scan(&id); err != nil {
		return 0, query_error(operation, query, err)
	}
	return id, nil
}

func delete(ctx context.Context, query sqlc.SQL, run query_row_func, log_sql log_func, operation string) (bool, error){
	sql, data, err := query.Compile()
	if err != nil {
		return false, compile_error(operation, err)
	}
	
	if debug_log {
		log_sql(sqlc.SQL_debug(query))
	}
	
	var id uint64
	if err = run(ctx, sql+" RETURNING id", data...).Scan(&id); err == nil {
		return false, nil
	}
	if No_rows_error(err) {
		return true, ErrNotFound
	}
	return false, query_error(operation, query, err)
}

func compile_error(operation string, err error) error {
	return &Error{operation+" compile: "+err.Error(), errors.Wrap(err, 1).ErrorStack()}
}

func query_error(operation string, query sqlc.SQL, err error) error {
	msg := sqlc.SQL_error(operation, query, err)
	stack := errors.Wrap(err, 1).ErrorStack()
	if ctx_canceled(err) {
		return &Timeout_error{msg, stack}
	}
	return &Error{msg, stack}
}