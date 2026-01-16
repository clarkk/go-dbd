package dbd

import (
	"log"
	"runtime"
	"context"
	"database/sql"
	"github.com/go-errors/errors"
	_ "github.com/go-sql-driver/mysql"
	"github.com/clarkk/go-dbd/sqlc"
)

var (
	db 			*sql.DB
	connected 	bool
)

func Connect(dsn string, conn_cpu int){
	if connected {
		panic("DB is already connected")
	}
	
	var err error
	if db, err = sql.Open("mysql", dsn); err != nil {
		log.Fatalf("Unable to parse DSN and connect to DB: %v", err)
	}
	
	db.SetMaxOpenConns(runtime.NumCPU() * conn_cpu)
	db.SetMaxIdleConns(10)
	db.SetConnMaxLifetime(60 * 30)
	db.SetConnMaxIdleTime(60 * 5)
	
	connected = true
	
	if !Ping() {
		log.Fatalf("Unable to connect to DB: %v", err)
	}
}

func Ping() bool {
	if err := db.Ping(); err != nil {
		connected = false
		return false
	}
	return true
}

func Exec(ctx context.Context, query sqlc.SQL) (sql.Result, error){
	sql, data, err := query.Compile()
	if err != nil {
		return nil, &Error{"DB execute compile: "+err.Error(), errors.Wrap(err, 0).ErrorStack()}
	}
	
	result, err := db.ExecContext(ctx, sql, data...)
	if err != nil {
		msg 	:= sqlc.SQL_error("DB execute", query, err)
		stack 	:= errors.Wrap(err, 0).ErrorStack()
		if ctx_canceled(err) {
			return nil, &Timeout_error{msg, stack}
		}
		return nil, &Error{msg, stack}
	}
	return result, nil
}

func Query_row(ctx context.Context, query sqlc.SQL, scan []any) (bool, error){
	sql, data, err := query.Compile()
	if err != nil {
		return false, &Error{"DB query row compile: "+err.Error(), errors.Wrap(err, 0).ErrorStack()}
	}
	
	if err := db.QueryRowContext(ctx, sql, data...).Scan(scan...); err != nil {
		if No_rows_error(err) {
			return true, ErrNotFound
		}
		msg := sqlc.SQL_error("DB query row", query, err)
		stack := errors.Wrap(err, 0).ErrorStack()
		if ctx_canceled(err) {
			return false, &Timeout_error{msg, stack}
		}
		return false, &Error{msg, stack}
	}
	return false, nil
}

func Query(ctx context.Context, query sqlc.SQL) (*sql.Rows, error){
	sql, data, err := query.Compile()
	if err != nil {
		return nil, &Error{"DB query compile: "+err.Error(), errors.Wrap(err, 0).ErrorStack()}
	}
	
	rows, err := db.QueryContext(ctx, sql, data...)
	if err != nil {
		msg := sqlc.SQL_error("DB query", query, err)
		stack := errors.Wrap(err, 0).ErrorStack()
		if ctx_canceled(err) {
			return nil, &Timeout_error{msg, stack}
		}
		return nil, &Error{msg, stack}
	}
	return rows, nil
}

func Insert(ctx context.Context, query sqlc.SQL) (uint64, error){
	var id uint64
	sql, data, err := query.Compile()
	if err != nil {
		return id, &Error{"DB insert compile: "+err.Error(), errors.Wrap(err, 0).ErrorStack()}
	}
	
	if err := db.QueryRowContext(ctx, sql+"RETURNING id", data...).Scan(&id); err != nil {
		msg := sqlc.SQL_error("DB insert", query, err)
		stack := errors.Wrap(err, 0).ErrorStack()
		if ctx_canceled(err) {
			return 0, &Timeout_error{msg, stack}
		}
		return 0, &Error{msg, stack}
	}
	return id, nil
}

func Update(ctx context.Context, query sqlc.SQL) (sql.Result, error){
	sql, data, err := query.Compile()
	if err != nil {
		return nil, &Error{"DB update compile: "+err.Error(), errors.Wrap(err, 0).ErrorStack()}
	}
	
	result, err := db.ExecContext(ctx, sql, data...)
	if err != nil {
		msg := sqlc.SQL_error("DB update", query, err)
		stack := errors.Wrap(err, 0).ErrorStack()
		if ctx_canceled(err) {
			return nil, &Timeout_error{msg, stack}
		}
		return nil, &Error{msg, stack}
	}
	return result, nil
}

func Delete(ctx context.Context, query sqlc.SQL) (bool, error){
	sql, data, err := query.Compile()
	if err != nil {
		return false, &Error{"DB delete compile: "+err.Error(), errors.Wrap(err, 0).ErrorStack()}
	}
	
	var id uint64
	if err := db.QueryRowContext(ctx, sql+"RETURNING id", data...).Scan(&id); err != nil {
		if No_rows_error(err) {
			return true, ErrNotFound
		}
		msg 	:= sqlc.SQL_error("DB delete", query, err)
		stack 	:= errors.Wrap(err, 0).ErrorStack()
		if ctx_canceled(err) {
			return false, &Timeout_error{msg, stack}
		}
		return false, &Error{msg, stack}
	}
	return false, nil
}

func Close(){
	db.Close()
}