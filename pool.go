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
	DRIVER 			= "mysql"
	
	SCHEMA_CHAR 	= "char"
	SCHEMA_INT 		= "int"
	SCHEMA_DEC 		= "decimal"
	
	CTX_TX ctx_key 	= ""
)

var (
	db 				*sql.DB
	connected 		bool
	
	schema 			= map[string]schemas{}
	
	integers 		= map[string]int{
		"tinyint":		int_pow(2, 8),
		"smallint":		int_pow(2, 16),
		"mediumint":	int_pow(2, 24),
		"int":			int_pow(2, 32),
		"bigint":		int_pow(2, 64),
	}
)

type (
	Schema struct {
		Type 		string
		Subtype 	string
		Length 		int
		Null 		bool
		Unsigned 	bool
		Length_dec 	int
		Range 		length_range
	}
	
	length_range struct {
		Min 	int
		Max		int
	}
	
	schemas 		map[string]Schema
	
	ctx_key 		string
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
	
	fetch_schema()
}

func Begin(ctx context.Context) *sql.Tx {
	tx, err := db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil && !ctx_canceled(err) {
		panic("DB transaction begin: "+err.Error())
	}
	return tx
}

func Apply(r *http.Request, tx *sql.Tx) *http.Request {
	return r.WithContext(context.WithValue(r.Context(), CTX_TX, tx))
}

func Applied(r *http.Request) *sql.Tx {
	return r.Context().Value(CTX_TX).(*sql.Tx)
}

func Close(){
	db.Close()
}

func ctx_canceled(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

func int_pow(n, m int) int {
	if m == 0 {
		return 1
	}
	
	result := n
	for i := 2; i <= m; i++ {
		result *= n
	}
	return result
}