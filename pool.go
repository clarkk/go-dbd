package dbd

import (
	"log"
	"runtime"
	"context"
	"database/sql"
	"github.com/go-errors/errors"
	_ "github.com/go-sql-driver/mysql"
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

func Query_row(ctx context.Context, sql string, data []any, scan []any) error {
	if err := db.QueryRowContext(ctx, sql, data...).Scan(scan...); err != nil {
		if ctx_canceled(err) {
			return &Timeout_error{"DB query row", err}
		}
		return &Error{"DB query row", err, errors.Wrap(err, 0).ErrorStack()}
	}
	return nil
}

func Close(){
	db.Close()
}