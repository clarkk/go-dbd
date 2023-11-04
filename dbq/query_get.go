package dbq

import (
	"strings"
	"strconv"
	"github.com/clarkk/go-dbd/dbv"
)

type (
	Select 			[]string
	Limit 			[]int
	
	Query_get struct {
		Query
		
		read_lock 			bool
		read_count 			bool
		
		in_select 			Select
		out_select 			select_clause
		
		in_limit 			Limit
	}
)

func Get(name string, view *dbv.View) *Query_get {
	return &Query_get{
		Query: Query{
			read:			true,
			view:			view,
			table:			view.Table(),
			table_name:		name,
		},
	}
}

//	Read-lock (SELECT ... FOR UPDATE)
func (q *Query_get) Lock(){
	q.read_lock = true
}

//	Count all entries without LIMIT and LEFT JOIN
func (q *Query_get) Count(){
	q.read_count = true
}

func (q *Query_get) Select(fields Select){
	q.in_select = fields
}

func (q *Query_get) Limit(fields Limit){
	q.in_limit = fields
}

func (q *Query_get) Write() (Error_code, error) {
	//	Check if table is private
	if q.public && !q.view.Public() {
		return q.error_table_private()
	}
	
	q.prepare()
	q.parse_select()
	q.parse_where()
	q.parse_limit()
	
	//	Check if select is empty
	if len(q.out_select) == 0 {
		return q.error_select_empty()
	}
	
	//	Check if selected by id (primary key)
	if q.read_lock && !q.read_id {
		return q.error_select_lock_id()
	}
	
	if code, err := q.error(); code != 0 {
		return code, err
	}
	
	q.create_sql()
	
	return 0, nil
}

func (q *Query_get) create_sql(){
	q.sql = "SELECT "+q.sql_select_clause()+"\nFROM "+q.sql_from_clause()
	if q.joined {
		q.sql += q.sql_joins()
	}
	if len(q.out_where) != 0 {
		q.sql += "\nWHERE "+q.sql_where_clause()
	}
	if !q.read_count {
		if len(q.in_limit) != 0 {
			q.sql += "\nLIMIT "+q.sql_limit_clause()
		}
		if q.read_lock {
			q.sql += "\nFOR UPDATE"
		}
	}
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

func (q *Query_get) parse_limit(){
	if len(q.in_limit) > 2 {
		q.error_limit_value()
	}
}

func (q *Query_get) sql_select_clause() string {
	if q.read_count {
		return "count(*)"
	}
	
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

func (q *Query_get) sql_joins() string {
	//	Only join inner joins on read count
	if q.read_count {
		if len(q.joins_inner) == 0 {
			return ""
		}
		return "\n"+strings.Join(q.joins_inner, "\n")
	}
	
	joins := append(q.joins_inner, q.joins...)
	if len(joins) == 0 {
		return ""
	}
	return "\n"+strings.Join(joins, "\n")
}

func (q *Query_get) sql_limit_clause() string {
	b := make([]string, len(q.in_limit))
	for i, v := range q.in_limit {
		b[i] = strconv.Itoa(v)
	}
	return strings.Join(b, ",")
}