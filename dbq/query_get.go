package dbq

import (
	"fmt"
	"strings"
	//"github.com/go-errors/errors"
	"github.com/clarkk/go-dbd/dbv"
)

type Query_get struct {
	Query
	
	in_select 		Select
	out_select 		select_clause
}

func NewQuery_get(name string, view *dbv.View) *Query_get {
	return &Query_get{
		Query: Query{
			view:		view,
			table:		view.Table(),
			table_name:	name,
		},
	}
}

func (q *Query_get) Select(fields Select){
	q.in_select = fields
}

func (q *Query_get) Write() (Error_code, error) {
	//	Check if table is private
	if q.public && !q.view.Public() {
		return q.error_table_private()
	}
	
	q.prepare()
	q.parse_select()
	q.parse_where()
	
	//	Check if select is empty
	if len(q.out_select) == 0 {
		return q.error_select_empty()
	}
	
	if code, err := q.error(); code != 0 {
		return code, err
	}
	
	//SQL_CALC_FOUND_ROWS
	
	q.sql = `SELECT `+q.sql_select_clause(q.out_select)+`
FROM `+q.table_name
	
	fmt.Println(q.sql)
	fmt.Println("select:", q.out_select)
	fmt.Println("where:", q.out_where)
	
	return 0, nil
}

func (q *Query_get) parse_select(){
	q.out_select = make(select_clause, len(q.in_select))
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
		
		q.out_select[k].sql_exp = q.field_translate(q.out_select[k].field)
	}
}

func (q *Query) sql_select_clause(values select_clause) string {
	sql := make([]string, len(values))
	for k, v := range values {
		sql[k] = v.field
	}
	return strings.Join(sql, ",")
}

/*func NewQuery_get(ctx context.Context, view dbt.View) *Query_get {
	return &Query_get{
		query: query{
			view:		view,
			table:		view.Table(),
		},
		ctx: ctx,
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