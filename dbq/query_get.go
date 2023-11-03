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

func Get(name string, view *dbv.View) *Query_get {
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
	//FOR UPDATE
	
	//	Create SQL query
	q.sql = "SELECT "+q.sql_select_clause()+"\nFROM "+q.sql_from_clause()
	if q.joined {
		q.sql += "\n"+q.sql_joins()
	}
	if len(q.out_where) != 0 {
		q.sql += "\n"+q.sql_where_clause()
	}
	
	return 0, nil
}

func (q *Query_get) parse_select(){
	q.out_select = make(select_clause, len(q.in_select))
	for k, v := range q.in_select {
		var field string
		
		//	Parse field
		if s1, s2, found := strings.Cut(v, "|"); found {
			q.out_select[k].fn 	= s1
			field 				= s2
		}else{
			field 				= v
		}
		
		//	Parse field as
		if s1, s2, found := strings.Cut(field, "="); found {
			field 						= s1
			q.out_select[k].field_as 	= s2
		}
		
		q.field_exists(field)
		
		q.out_select[k].field = field
		
		if q.error_code != 0 {
			continue
		}
		
		q.out_select[k].sql_exp = q.field_translate(field)
	}
}

func (q *Query_get) sql_select_clause() string {
	sql := make([]string, len(q.out_select))
	for k, v := range q.out_select {
		var col string
		if q.joined {
			col = v.table_as+"."+v.col
		}else{
			col = v.col
		}
		
		//	Apply function
		if v.fn != "" {
			col = v.fn+"("+col+")"
		}
		
		//	Apply "field as"
		if v.field_as != "" {
			col += " "+v.field_as
		//	Renamed in table map
		}else if v.field != v.col {
			col += " "+v.field
		}
		
		sql[k] = col
	}
	return strings.Join(sql, ",")
}