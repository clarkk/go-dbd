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

func Query_row(ctx context.Context, query sqlc.SQL, scan []any) error {
	sql, err := query.Compile()
	if err != nil {
		return &Error{"DB query row compile: "+err.Error(), errors.Wrap(err, 0).ErrorStack()}
	}
	
	if err := db.QueryRowContext(ctx, sql, query.Data()...).Scan(scan...); err != nil {
		msg := "DB query row: "+err.Error()
		stack := errors.Wrap(err, 0).ErrorStack()
		if ctx_canceled(err) {
			return &Timeout_error{msg, stack}
		}
		return &Error{msg, stack}
	}
	return nil
}

func Insert(ctx context.Context, query sqlc.SQL) (int, error){
	var id int
	sql, err := query.Compile()
	if err != nil {
		return id, &Error{"DB insert compile: "+err.Error(), errors.Wrap(err, 0).ErrorStack()}
	}
	
	if err := db.QueryRowContext(ctx, sql+" RETURNING id", query.Data()...).Scan(&id); err != nil {
		msg := "DB insert: "+err.Error()
		stack := errors.Wrap(err, 0).ErrorStack()
		if ctx_canceled(err) {
			return 0, &Timeout_error{msg, stack}
		}
		return 0, &Error{msg, stack}
	}
	return id, nil
}

func Close(){
	db.Close()
}