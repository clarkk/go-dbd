package dbc

import (
	"context"
	"database/sql"
	"github.com/clarkk/go-dbd/dbq"
)

type get struct {
	ctx 	context.Context
	query 	*dbq.Query_get
	stmt 	*sql.Stmt
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
	/*var err error
	sql := "SELECT id, timeout, lang FROM block WHERE id=?"
	q.stmt, err = tx.PrepareContext(q.ctx, sql)
	if err != nil {
		panic("SQL prepare "+sql+": "+err.Error())
	}*/
	
	return c.query.Write()
}

func (c *get) Close(){
	if c.stmt != nil {
		c.stmt.Close()
	}
}