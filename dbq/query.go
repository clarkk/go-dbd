package dbq

import(
	"strings"
	"github.com/clarkk/go-dbd/dbt"
	"github.com/clarkk/go-dbd/dbv"
)

const (
	RUNE_START 	= 97	// a
	RUNE_END 	= 122	// z
)

/*var (
	sql_operator_in = map[string]string{
		"in":	"IN (?)",
		"!in":	"NOT IN (?)",
	}
	
	sql_operator_between = map[string]string{
		"bt":	"BETWEEN ? AND ?",
		"!bt":	"NOT BETWEEN ? AND ?",
	}
)*/

type (
	Select 			[]string
	Where 			map[string]interface{}
	
	Query struct {
		view 			*dbv.View
		table 			*dbt.Table
		table_name 		string
		
		public 			bool
		
		table_as_i 		rune
		table_as_map 	map[string]string
		
		joined 			bool
		joins 			[]string
		
		in_where 		Where
		out_where 		where_clause
		
		sql 			string
		
		error_code 		Error_code
		invalid_fields 	map[string]string
		invalid_where 	[]string
	}
	
	select_field struct {
		sql_exp
		fn 			string
		col_as 		string
	}
	
	where_field struct {
		sql_exp
		fn 			string
		op 			string
		value 		string
	}
	
	sql_exp struct {
		table 		string
		table_as 	string
		col 		string
	}
	
	select_clause 	[]select_field
	where_clause 	[]where_field
)

func (q *Query) Public(){
	q.public = true
}

func (q *Query) Where(fields Where){
	q.in_where = fields
}

func (q *Query_get) SQL() string {
	return q.sql
}

func (q *Query) prepare(){
	q.table_as_map 		= map[string]string{}
	q.invalid_fields 	= map[string]string{}
	q.joins 			= []string{}
}

func (q *Query) parse_where(){
	q.out_where = make(where_clause, len(q.in_where))
	i := 0
	for k, v := range q.in_where {
		var field string
		
		//	Parse field
		if s1, s2, found := strings.Cut(k, "|"); found {
			q.out_where[i].fn 	= s1
			field 				= s2
		}else{
			field 				= k
		}
		if s1, s2, found := strings.Cut(field, " "); found {
			field 				= s1
			q.out_where[i].op 	= s2
		}
		
		//	Check where value
		var ok bool
		q.out_where[i].value, ok = v.(string)
		if !ok {
			q.error_where_value(field)
		}
		
		q.field_exists(field)
		
		q.out_where[i].col = field
		
		if q.error_code != 0 {
			i++
			continue
		}
		
		q.out_where[i].sql_exp = q.field_translate(field)
		i++
	}
}

func (q *Query) sql_from_clause() string {
	if q.joined {
		return "."+q.table_name+" "+q.table_as(q.table_name)
	}
	return "."+q.table_name
}

func (q *Query) field_exists(name string){
	if q.public && !q.table.Exists_public(name) {
		q.error_invalid_field(name)
	}
	
	if !q.table.Exists(name) {
		q.error_invalid_field(name)
	}
}

func (q *Query) field_translate(name string) sql_exp {
	//	Joined tables
	var sql sql_exp
	if q.table.Joined(name) {
		q.joined = true
		
		//q.joins = 
		
		sql = sql_exp{
			table:		q.table.Table(name),
			table_as:	q.table_as(q.table.Table(name)),
			col:		q.table.Col(name),
		}
	}else{
		sql = sql_exp{
			table:		q.table_name,
			table_as:	q.table_as(q.table_name),
			col:		q.table.Col(name),
		}
	}
	return sql
}

func (q *Query) table_as(name string) string {
	if r, found := q.table_as_map[name]; found {
		return string(r)
	}
	
	switch q.table_as_i {
	case 0:
		q.table_as_i = RUNE_START
	case RUNE_END:
		panic("Table joins exceeded limit")
	default:
		q.table_as_i++
	}
	q.table_as_map[name] = string(q.table_as_i)
	return q.table_as_map[name]
}