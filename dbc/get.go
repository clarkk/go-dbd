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

//	Read-lock: SELECT ... FOR UPDATE
func (c *get) Read_lock() *get {
	c.query.Read_lock()
	return c
}

//	Count all entries without LIMIT and LEFT JOIN
func (c *get) Count() *get {
	c.query.Count()
	return c
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

func (c *get) Where_in(fields dbq.Where) *get {
	c.query.Where_in(fields)
	return c
}

func (c *get) Limit(fields dbq.Limit) *get {
	c.query.Limit(fields)
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

/*rows, err := stmt.QueryContext(ctx, 1)
if err != nil {
	log.Fatal(err)
}
defer rows.Close()

cols, _ := rows.Columns()
cols_len := len(cols)
for rows.Next() {
	columns 	:= make([]interface{}, cols_len)
	columns_ref := make([]interface{}, cols_len)
	for i, _ := range columns {
		columns_ref[i] = &columns[i]
	}
	
	if err := rows.Scan(columns_ref...); err != nil {
		log.Fatal(err)
	}
	
	m := make(map[string]interface{})
	for i, col_name := range cols {
		val := columns_ref[i].(*interface{})
		m[col_name] = *val
	}
	
	fmt.Println(m)
}
if err = rows.Err(); err != nil {
	log.Fatal(err)
}*/