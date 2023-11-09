package dbq

import(
	//"sort"
	"strings"
	"context"
	//"database/sql"
	"github.com/clarkk/go-dbd/dbt"
	"github.com/clarkk/go-dbd/dbv"
	//"github.com/clarkk/go-util/sutil"
)

const (
	OP_IN op_mode 	= 1
	OP_BT op_mode 	= 2
	
	RUNE_START 		= 97	// a
	RUNE_END 		= 122	// z
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
	Where 			map[string]any
	Where_op 		[]int
	
	//Prepared_values map[string]any
	
	query struct {
		ctx 			context.Context
		view 			*dbv.View
		table 			*dbt.Table
		table_name 		string
		
		//rows 			*sql.Rows
		//stmt 			*sql.Stmt
		
		read 			bool
		read_id 		bool
		
		public 			bool
		
		table_as_i 		rune
		table_as_map 	map[string]string
		
		joined 			bool
		joins 			[]string
		
		in_where 		Where
		out_where 		where_clause
		
		/*sql 			string
		sql_fields 		[]string
		sql_values 		[]any*/
		
		error_code 				Error_code
		invalid_fields 			map[string]string
		invalid_where 			[]string
		invalid_where_operator 	[]string
		invalid_order_mode 		[]string
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
		op_mode 	op_mode
		op_exp 		string
		value 		any
		value_op 	Where_op
	}
	
	order_field struct {
		sql_exp
		field 		string
		desc 		bool
	}
	
	sql_exp struct {
		table 		string
		table_as 	string
		col 		string
	}
	
	select_clause 	[]select_field
	where_clause 	[]where_field
	order_clause 	[]order_field
	
	op_mode 		int8
)

func (q *query) init(){
	q.table_as_map 		= map[string]string{}
	q.invalid_fields 	= map[string]string{}
}

func (q *query) parse_where(){
	q.out_where = make(where_clause, len(q.in_where))
	i := 0
	for k, v := range q.in_where {
		var (
			field 	string
			op_exp 	string
		)
		
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
				q.out_where[i].op 			= s2
			default:
				var found bool
				if op_exp, found = sql_operator_in[s2]; found {
					q.out_where[i].op 		= s2
					q.out_where[i].op_mode 	= OP_IN
					q.out_where[i].op_exp 	= op_exp
				}else if op_exp, found = sql_operator_between[s2]; found {
					q.out_where[i].op 		= s2
					q.out_where[i].op_mode 	= OP_BT
					q.out_where[i].op_exp 	= op_exp
				}else{
					q.error_where_operator(field, s2)
				}
			}
			field = s1
		}
		
		//	Validate where value
		if op_exp != "" {
			switch value := v.(type) {
			case Where_op:
				if q.out_where[i].op_mode == OP_BT {
					if len(value) == 2 {
						if value[0] > value[1] {
							q.error_where_value(field)
						}
					}else{
						q.error_where_value(field)
					}
				}
				q.out_where[i].value_op 	= value
			default:
				q.error_where_value(field)
			}
		}else{
			switch value := v.(type) {
			case string, int:
				q.out_where[i].value = value
			default:
				q.error_where_value(field)
			}
		}
		
		q.field_exists(field)
		
		//	Check if selected by id (primary key)
		if q.read && field == "id" && q.out_where[i].op == "" {
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

func (q *query) field_translate(name string) sql_exp {
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
		q.joins 	= append(q.joins, join_sql)
		return sql
	}
	
	return sql_exp{
		table:		q.table_name,
		table_as:	q.table_as(q.table_name),
		col:		q.table.Col(name),
	}
}

func (q *query) table_as(name string) string {
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

func (q *query) field_exists(name string){
	if q.public && !q.table.Exists_public(name) {
		q.error_invalid_field(name)
	}
	
	if !q.table.Exists(name) {
		q.error_invalid_field(name)
	}
}

/*func (q *Query) Prepare(tx *sql.Tx) error {
	var err error
	if q.stmt, err = tx.PrepareContext(q.ctx, q.sql); err != nil {
		return err
	}
	sort.Strings(q.sql_fields)
	return nil
}

func (q *Query) Close(){
	if q.rows != nil {
		q.rows.Close()
	}
	if q.stmt != nil {
		q.stmt.Close()
	}
}

func (q *Query) sql_from_clause() string {
	from := "."+q.table_name
	if q.read && q.joined {
		from += " "+q.table_as(q.table_name)
	}
	return from
}

func (q *Query) sql_where_clause() string {
	sql := make([]string, len(q.out_where))
	for k, v := range q.out_where {
		col := q.sql_col(v.sql_exp)
		
		//	Apply function
		if v.fn != "" {
			col = v.fn+"("+col+")"
		}
		
		//	Apply operator
		if v.op_exp != "" {
			col += " "+v.op_exp
			switch v.op_mode {
			case OP_IN:
				q.apply_sql_value(v.field, sutil.Int_csv(v.value_op))
			case OP_BT:
				q.apply_sql_value(v.field, v.value_op[0])
				q.apply_sql_value(v.field, v.value_op[1])
			}
		}else{
			col += v.op+"=?"
			q.apply_sql_value(v.field, v.value)
		}
		
		sql[k] = col
	}
	return strings.Join(sql, " && ")
}

func (q *Query) sql_col(v sql_exp) string {
	var col string
	if q.joined {
		col = v.table_as+"."
	}
	return col+v.col
}

func (q *Query) apply_sql_value(field string, value any){
	q.sql_fields = append(q.sql_fields, field)
	q.sql_values = append(q.sql_values, value)
}*/