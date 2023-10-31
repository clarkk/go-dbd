package dbd

import (
	"context"
	"database/sql"
)

type query_get struct {
	query
	ctx 		context.Context
	stmt 		*sql.Stmt
}

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
	
	/*table 	:= q.view.Table()
	as 		:= q.view.As()
	fields 	:= table.Fields()
	joins 	:= table.Joins()
	get 	:= table.Get()
	fmt.Println("table:", as, fields, joins, get)*/
	
	q.parse_select()
	q.parse_where()
	
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