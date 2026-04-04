package dbd

import (
	"log"
	"strings"
)

var debug_log bool

func Debug_log(){
	debug_log = true
}

func log_sql(sql string){
	indented := strings.ReplaceAll(sql, "\n", "\n\t")
	log.Printf("[DB LOG] %s", indented)
}