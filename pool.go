package dbd

import (
	"log"
	"time"
	"context"
	"runtime"
	"net/http"
	"database/sql"
	"github.com/go-errors/errors"
	_ "github.com/go-sql-driver/mysql"
)

const (
	DRIVER = "mysql"
	
	CTX_TX 	ctx_key = ""
)

var (
	db 			*sql.DB
	connected 	bool
)

type (
	ctx_key 	string
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
	tx, err := db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil && !ctx_canceled(err) {
		panic("DB transaction begin: "+err.Error())
	}
	return tx
}

func Wrap(r *http.Request, tx *sql.Tx) *http.Request {
	return r.WithContext(context.WithValue(r.Context(), CTX_TX, tx))
}

func Wrapped(r *http.Request) *sql.Tx {
	return r.Context().Value(CTX_TX).(*sql.Tx)
}

func Close(){
	db.Close()
}

func ctx_canceled(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}