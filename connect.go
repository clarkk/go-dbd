package dbd

import (
	"log"
	"time"
	"context"
	"runtime"
	"database/sql"
	"github.com/go-errors/errors"
	_ "github.com/go-sql-driver/mysql"
)

const (
	DRIVER 		= "mysql"
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
	db, err = sql.Open(DRIVER, dsn)
	if err != nil {
		log.Fatal("Could no parse DB DSN: "+err.Error())
	}
	
	db.SetConnMaxLifetime(time.Minute * 30)
	db.SetMaxOpenConns(runtime.NumCPU() * conn_cpu)
	db.SetConnMaxIdleTime(30)
	
	err = db.Ping()
	if err != nil {
		log.Fatal("Unable to connect to DB: "+err.Error())
	}
	
	connected = true
}

func Begin(ctx context.Context) *sql.Tx {
	tx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil && !ctx_canceled(err) {
		panic(err)
	}
	return tx
}

func Close(){
	db.Close()
}

func ctx_canceled(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}