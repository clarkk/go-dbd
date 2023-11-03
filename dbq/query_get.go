package dbq

import (
	"strings"
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
FROM `+q.sql_from_clause()
	
	if q.joined {
		
		//q.sql += "\n"
	}
	
	return 0, nil
}

func (q *Query_get) parse_select(){
	q.out_select = make(select_clause, len(q.in_select))
	for k, v := range q.in_select {
		var field string
		
		//	Parse field
		if s1, s2, found := strings.Cut(v, "|"); found {
			q.out_select[k].fn 		= s1
			field 					= s2
		}else{
			field 					= v
		}
		if s1, s2, found := strings.Cut(field, "="); found {
			field 					= s1
			q.out_select[k].col_as 	= s2
		}
		
		q.field_exists(field)
		
		q.out_select[k].col = field
		
		if q.error_code != 0 {
			continue
		}
		
		q.out_select[k].sql_exp = q.field_translate(field)
	}
}

func (q *Query) sql_select_clause(values select_clause) string {
	sql := make([]string, len(values))
	for k, v := range values {
		if q.joined {
			sql[k] = v.table_as+"."+v.col
		}else{
			sql[k] = v.col
		}
	}
	return strings.Join(sql, ",")
}