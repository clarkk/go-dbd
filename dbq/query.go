package dbq

import(
	"fmt"
	"strings"
	"context"
	"database/sql"
	//"github.com/go-errors/errors"
	"github.com/clarkk/go-dbd/dbt"
	"github.com/clarkk/go-dbd/dbv"
)

const (
	RUNE_START 	= 97
	RUNE_END 	= 122
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
	Where 			map[string]string
	
	Query struct {
		ctx 			context.Context
		view 			*dbv.View
		table 			*dbt.Table
		table_name 		string
		
		public 			bool
		
		table_as_i 		rune
		table_as_map 	map[string]string
		
		in_where 		Where
		
		stmt 			*sql.Stmt
		
		error_code 		Error_code
		invalid_fields 	map[string]string
		
		out_where 		where_clause
		
		joined 			bool
	}
	
	sql_exp struct {
		table 		string
		abbr 		string
		col 		string
	}
	
	select_field struct {
		sql_exp
		fn 			string
		field 		string
		as 			string
	}
	
	select_clause 	[]select_field
	
	where_field struct {
		sql_exp
		fn 			string
		field 		string
		op 			string
		value 		string
	}
	
	where_clause 	[]where_field
)

func (q *Query) Public(){
	q.public = true
}

func (q *Query) Where(fields Where){
	q.in_where = fields
}

func (q *Query) Close(){
	if q.stmt != nil {
		q.stmt.Close()
	}
}

func (q *Query) prepare(){
	q.table_as_map 		= map[string]string{}
	q.invalid_fields 	= map[string]string{}
}

func (q *Query) parse_where(){
	q.out_where = make(where_clause, len(q.in_where))
	i := 0
	for k, v := range q.in_where {
		q.out_where[i].value = v
		
		//	Parse field
		if s1, s2, found := strings.Cut(k, "|"); found {
			q.out_where[i].fn 		= s1
			q.out_where[i].field 	= s2
		}else{
			q.out_where[i].field 	= k
		}
		if s1, s2, found := strings.Cut(q.out_where[i].field, " "); found {
			q.out_where[i].field 	= s1
			q.out_where[i].op 		= s2
		}
		fmt.Println(q.out_where[i])
		q.field_exists(q.out_where[i].field)
		
		i++
		
		if q.error_code != 0 {
			continue
		}
		
		//q.out_where[i].sql_exp = q.field_translate(q.out_where[i].field)
	}
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
		
		sql = sql_exp{
			table:	q.table.Table(name),
			abbr:	q.table_as(q.table.Table(name)),
			col:	q.table.Col(name),
		}
	}else{
		sql = sql_exp{
			table:	q.table_name,
			abbr:	q.table_as(q.table_name),
			col:	q.table.Col(name),
		}
	}
	
	return sql
}

func (q *Query) table_as(name string) string {
	if r, found := q.table_as_map[name]; found {
		return string(r)
	}else{
		switch q.table_as_i {
		case 0:
			q.table_as_i = RUNE_START
		case RUNE_END:
			panic("Table join exceeded map limit")
		default:
			q.table_as_i++
		}
		q.table_as_map[name] = string(q.table_as_i)
		return q.table_as_map[name]
	}
}