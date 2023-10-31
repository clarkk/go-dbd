package dbd

import (
	"fmt"
	"strings"
	"context"
	"database/sql"
	"github.com/clarkk/go-dbd/dbt"
)

type (
	query struct {
		in_select 	Select
		in_where 	Where
		
		out_select 	select_clause
	}
	
	query_get struct {
		query
		ctx 		context.Context
		public 		bool
		view 		dbt.View
		stmt 		*sql.Stmt
	}
)

func (q *query_get) Public() *query_get {
	q.public = true
	return q
}

func (q *query_get) Select(fields Select) *query_get {
	q.in_select = fields
	return q
}

func (q *query_get) Where(fields Where) *query_get {
	q.in_where = fields
	return q
}

func (q *query_get) Prepare(tx *sql.Tx) error {
	//	Check if table is private
	if q.public && !q.view.Public() {
		return ERR_PRIVATE
	}
	
	q.parse_select()
	fmt.Println("select out:", q.out_select)
	//g.parse_where()
	//fmt.Println("where:", g.in_where)
	
	var err error
	sql := "SELECT id, timeout, lang FROM block WHERE id=?"
	q.stmt, err = tx.PrepareContext(q.ctx, sql)
	if err != nil {
		panic("SQL prepare "+sql+": "+err.Error())
	}
	return nil
}

func (q *query_get) Close(){
	q.stmt.Close()
}

func (q *query_get) parse_select(){
	q.out_select = make(select_clause, len(q.in_select))
	for k, v := range q.in_select {
		if s1, s2, found := strings.Cut(v, "|"); found {
			q.out_select[k].fn 		= s1
			q.out_select[k].field 	= s2
		}else{
			q.out_select[k].field 	= v
		}
		
		if s1, s2, found := strings.Cut(q.out_select[k].field, "="); found {
			q.out_select[k].field 	= s1
			q.out_select[k].as 		= s2
		}
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