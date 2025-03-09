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

func Query_row(ctx context.Context, query sqlc.SQL, scan []any) (bool, error){
	sql, err := query.Compile()
	if err != nil {
		return false, &Error{"DB query row compile: "+err.Error(), errors.Wrap(err, 0).ErrorStack()}
	}
	
	if err := db.QueryRowContext(ctx, sql, query.Data()...).Scan(scan...); err != nil {
		if Is_empty_error(err) {
			return true, nil
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

func Insert(ctx context.Context, query sqlc.SQL) (uint64, error){
	var id uint64
	sql, err := query.Compile()
	if err != nil {
		return id, &Error{"DB insert compile: "+err.Error(), errors.Wrap(err, 0).ErrorStack()}
	}
	
	if err := db.QueryRowContext(ctx, sql+"RETURNING id", query.Data()...).Scan(&id); err != nil {
		msg := sqlc.SQL_error("DB insert", query, err)
		stack := errors.Wrap(err, 0).ErrorStack()
		if ctx_canceled(err) {
			return 0, &Timeout_error{msg, stack}
		}
		return 0, &Error{msg, stack}
	}
	return id, nil
}

func Update(ctx context.Context, query sqlc.SQL) error {
	sql, err := query.Compile()
	if err != nil {
		return &Error{"DB update compile: "+err.Error(), errors.Wrap(err, 0).ErrorStack()}
	}
	
	if _, err := db.ExecContext(ctx, sql, query.Data()...); err != nil {
		msg := sqlc.SQL_error("DB update", query, err)
		stack := errors.Wrap(err, 0).ErrorStack()
		if ctx_canceled(err) {
			return &Timeout_error{msg, stack}
		}
		return &Error{msg, stack}
	}
	return nil
}

func Close(){
	db.Close()
}