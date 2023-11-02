package dbq

import (
	"fmt"
	"context"
	"database/sql"
	//"github.com/go-errors/errors"
	"github.com/clarkk/go-dbd/dbv"
)

type Query_get struct {
	Query
	
	in_select 		Select
	out_select 		select_clause
}

func NewQuery_get(ctx context.Context, table string, views dbv.Views) *Query_get {
	return &Query_get{
		Query: Query{
			ctx:		ctx,
			table_name:	table,
			views:		views,
		},
	}
}

func (q *Query_get) Select(fields Select){
	q.in_select = fields
}

func (q *Query_get) Prepare(tx *sql.Tx) (Error_code, error) {
	//	Check if table exists
	var found bool
	q.view, found = q.views[q.table_name]
	if !found {
		return q.error_table(q.table_name)
	}
	
	//	Check if table is private
	if q.public && !q.view.Public() {
		return q.error_table_private()
	}
	
	q.prepare()
	q.parse_select()
	
	fmt.Println("ok")
	
	
	
	//table 	:= q.view.Table()
	//as 		:= q.view.As()
	//fields 	:= table.Fields()
	//joins 	:= table.Joins()
	//get 	:= table.Get()
	//fmt.Println("table:", as, fields, joins, get)
	
	/*
	
	q.parse_where()
	
	if code, err := q.error(); code != 0 {
		return code, err
	}
	
	var err error
	sql := "SELECT id, timeout, lang FROM block WHERE id=?"
	q.stmt, err = tx.PrepareContext(q.ctx, sql)
	if err != nil {
		panic("SQL prepare "+sql+": "+err.Error())
	}*/
	return 0, nil
}

func (q *Query) parse_select(){
	/*q.out_select = make(select_clause, len(q.in_select))
	for k, v := range q.in_select {
		//	Parse field
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
		
		q.field_exists(q.out_select[k].field)
		
		if q.error_code != 0 {
			continue
		}
		
		q.field_translate(q.out_select[k].field)
	}*/
}

/*func NewQuery_get(ctx context.Context, view dbt.View) *Query_get {
	return &Query_get{
		query: query{
			view:		view,
			table:		view.Table(),
		},
		ctx: ctx,
	}
}

func (q *Query_get) Close(){
	if q.stmt != nil {
		q.stmt.Close()
	}
}*/

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