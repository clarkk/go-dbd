package dbq

import(
	"fmt"
	"strings"
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
	Select 			[]string
	Where 			map[string]string
	
	query struct {
		public 			bool
		
		view 			dbt.View
		table 			*dbt.Table
		table_as 		string
		
		in_select 		Select
		in_where 		Where
		
		out_select 		select_clause
		out_where 		where_clause
		
		joined 			bool
		
		error_code 		error_code
		invalid_fields 	map[string]string
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
		
		q.field_translate(q.out_select[k].field)
	}
}

func (q *query) parse_where(){
	q.out_where = make(where_clause, len(q.in_where))
	for k, v := range q.in_where {
		fmt.Println("where:", k, v)
	}
}

func (q *query) field_translate(name string){
	//	Joined tables
	if q.table.Joined(name) {
		q.joined = true
		
		fmt.Println(q.table_as, q.table.Col(name))
	}else{
		fmt.Println(q.table_as, q.table.Col(name))
	}
}

func (q *query) field_exists(name string){
	if q.public && !q.table.Exists_public(name) {
		q.error_invalid_field(name)
	}
	
	if !q.table.Exists(name) {
		q.error_invalid_field(name)
	}
}