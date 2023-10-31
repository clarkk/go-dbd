package dbd

import (
	"fmt"
	"context"
	"database/sql"
	"github.com/clarkk/go-dbd/dbt"
)

type Get struct {
	ctx 		context.Context
	public 		bool
	view 		dbt.View
	
	sql_select 	Select
	sql_where 	Where
	
	stmt 		*sql.Stmt
}

func (g *Get) Public() *Get {
	g.public = true
	return g
}

func (g *Get) Select(fields Select) *Get {
	g.sql_select = fields
	return g
}

func (g *Get) Where(fields Where) *Get {
	g.sql_where = fields
	return g
}

func (g *Get) Prepare(tx *sql.Tx) error {
	if g.public && !g.view.Public() {
		return ERR_PRIVATE
	}
	
	fmt.Println("select:", g.sql_select)
	fmt.Println("where:", g.sql_where)
	
	var err error
	sql := "SELECT id, timeout, lang FROM block WHERE id=?"
	g.stmt, err = tx.PrepareContext(g.ctx, sql)
	if err != nil {
		panic("SQL prepare "+sql+": "+err.Error())
	}
	return nil
}

func (g *Get) Result() (bool, error) {
	if g.public && !g.view.Public() {
		return false, ERR_PRIVATE
	}
	
	fmt.Println(g.view.Table())
	
	return true, nil
	
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
}

func (g *Get) Close(){
	g.stmt.Close()
}