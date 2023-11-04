package dbq

import(
	"strings"
	"strconv"
	"github.com/clarkk/go-dbd/dbt"
	"github.com/clarkk/go-dbd/dbv"
)

const (
	INNER_JOIN 	= "INNER JOIN"
	
	RUNE_START 	= 97	// a
	RUNE_END 	= 122	// z
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
	Where 			map[string]interface{}
	
	Query struct {
		view 			*dbv.View
		table 			*dbt.Table
		table_name 		string
		
		read 			bool
		read_id 		bool
		
		public 			bool
		
		table_as_i 		rune
		table_as_map 	map[string]string
		
		joined 			bool
		joins 			[]string
		joins_inner 	[]string
		
		in_where 		Where
		in_where_in 	Where
		out_where 		where_clause
		
		sql 			string
		
		error_code 				Error_code
		invalid_fields 			map[string]string
		invalid_where 			[]string
		invalid_where_operator 	[]string
	}
	
	select_field struct {
		sql_exp
		fn 			string
		field 		string
		field_as 	string
	}
	
	where_field struct {
		sql_exp
		fn 			string
		field 		string
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

func (q *Query) Where_in(fields Where){
	q.in_where_in = fields
}

func (q *Query) SQL() string {
	return q.sql
}

func (q *Query) prepare(){
	q.table_as_map 		= map[string]string{}
	q.invalid_fields 	= map[string]string{}
	q.joins 			= []string{}
	q.joins_inner 		= []string{}
}

func (q *Query) parse_where(){
	q.out_where = make(where_clause, len(q.in_where))
	i := 0
	for k, v := range q.in_where {
		var field string
		
		//	Parse function
		if s1, s2, found := strings.Cut(k, "|"); found {
			q.out_where[i].fn 	= s1
			field 				= s2
		}else{
			field 				= k
		}
		
		//	Parse operator
		if s1, s2, found := strings.Cut(field, " "); found {
			switch s2 {
			case "!", ">", "<":
				q.out_where[i].op = s2
			default:
				q.error_where_operator(field, s2)
			}
			field = s1
		}
		
		//	Check where value
		switch value := v.(type) {
		case string:
			q.out_where[i].value = value
		case int:
			q.out_where[i].value = strconv.Itoa(value)
		default:
			q.error_where_value(field)
		}
		
		q.field_exists(field)
		
		//	Check if selected by id (primary key)
		if q.read && field == "id" {
			q.read_id = true
		}
		
		q.out_where[i].field = field
		
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

func (q *Query) sql_where_clause() string {
	sql := make([]string, len(q.out_where))
	for k, v := range q.out_where {
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
		
		//	Apply operator
		col += v.op+"=?"
		
		sql[k] = col
	}
	return strings.Join(sql, ",")
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
	//	Joined tables (only select/read)
	if q.read && q.table.Joined(name) {
		q.joined = true
		sql := sql_exp{
			table:		q.table.Table(name),
			table_as:	q.table_as(q.table.Table(name)),
			col:		q.table.Col(name),
		}
		
		join 		:= q.table.Join(sql.table)
		join_mode 	:= string(join.Mode)
		join_sql 	:= join_mode+" ."+sql.table+" "+sql.table_as+" ON "+q.table_as(q.table_name)+"."+join.Col+"="+sql.table_as+"."+join.Foreign
		if join.Mode == INNER_JOIN {
			q.joins_inner 	= append(q.joins_inner, join_sql)
		}else{
			q.joins 		= append(q.joins, join_sql)
		}
		return sql
	}
	
	return sql_exp{
		table:		q.table_name,
		table_as:	q.table_as(q.table_name),
		col:		q.table.Col(name),
	}
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