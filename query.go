package dbd

import(
	"fmt"
	"strings"
	"slices"
	//"github.com/go-errors/errors"
	"github.com/clarkk/go-dbd/dbt"
)

var (
	sql_operator_in = map[string]string{
		"in":	"IN (?)",
		"!in":	"NOT IN (?)",
	}
	
	sql_operator_between = map[string]string{
		"bt":	"BETWEEN ? AND ?",
		"!bt":	"NOT BETWEEN ? AND ?",
	}
)

type (
	query struct {
		public 		bool
		view 		dbt.View
		
		in_select 	Select
		in_where 	Where
		
		out_select 	select_clause
		out_where 	where_clause
		
		error_code 			error_code
		invalid_fields 		map[string]string
	}
	
	select_field struct {
		fn 			string
		field 		string
		as 			string
	}
	
	where_field struct {
		clause 		string
		field 		string
		as 			string
	}
	
	select_clause 	[]select_field
	where_clause 	[]where_field
	
	error_code 		uint8
)

func (q *query) parse_select(){
	table 	:= q.view.Table()
	fields 	:= table.Fields()
	get 	:= table.Get()
	
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
		
		//	Check if field exists
		if q.public {
			if !slices.Contains(get, q.out_select[k].field) {
				q.error_invalid_field(q.out_select[k].field)
			}
		}else{
			if _, found := fields[q.out_select[k].field]; !found {
				q.error_invalid_field(q.out_select[k].field)
			}
		}
		
		if q.error_code != 0 {
			continue
		}
		
		//	Translate field
		
	}
}

func (q *query) parse_where(){
	q.out_where = make(where_clause, len(q.in_where))
	for k, v := range q.in_where {
		fmt.Println("where:", k, v)
	}
}