package dbc

import (
	"database/sql"
	"github.com/clarkk/go-dbd/dbq"
)

type get struct {
	query 	*dbq.Query_get
}

func (c *get) Public() *get {
	c.query.Public()
	return c
}

func (c *get) Select(fields dbq.Select) *get {
	c.query.Select(fields)
	return c
}

func (c *get) Where(fields dbq.Where) *get {
	c.query.Where(fields)
	return c
}

func (c *get) Prepare(tx *sql.Tx) (dbq.Error_code, error) {
	return c.query.Prepare(tx)
}

func (c *get) Close(){
	c.query.Close()
}