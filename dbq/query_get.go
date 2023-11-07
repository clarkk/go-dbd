package dbq

import (
	"fmt"
	"strings"
	"context"
	"database/sql"
	"github.com/clarkk/go-dbd/dbv"
)

type (
	Select 			[]string
	Limit 			[]int
	
	Row_result 		map[string]any
	
	Query_get struct {
		Query
		
		read_lock 			bool
		read_count 			bool
		
		in_select 			Select
		out_select 			select_clause
		
		in_limit 			Limit
		
		res_cols 			[]string
		res_cols_num 		int
		
		row 				Row_result
		row_error 			error
	}
)

func Get(ctx context.Context, name string, view *dbv.View) *Query_get {
	return &Query_get{
		Query: Query{
			ctx:			ctx,
			view:			view,
			table:			view.Table(),
			table_name:		name,
			read:			true,
		},
	}
}

//	Read-lock: SELECT ... FOR UPDATE
func (q *Query_get) Read_lock(){
	q.read_lock = true
}

//	Count all entries with SELECT COUNT(*) and without LIMIT
func (q *Query_get) Count(tx *sql.Tx) (int, error) {
	q.read_count = true
	q.compile_sql()
	var cnt int
	row := tx.QueryRowContext(q.ctx, q.sql, q.sql_values...)
	if err := row.Scan(&cnt); err != nil {
		return 0, err
	}
	return cnt, nil
}

func (q *Query_get) Select(fields Select){
	q.in_select = fields
}

func (q *Query_get) Limit(fields Limit){
	q.in_limit = fields
}

func (q *Query_get) Compile() (Error_code, error) {
	if error_code, err := q.prepare_select(); error_code != 0 {
		return error_code, err
	}
	return ERR_CODE_SUCCESS, nil
}

/*func (q *Query_get) Prepare(tx *sql.Tx) (Error_code, error) {
	if error_code, err := q.prepare_select(); error_code != 0 {
		return error_code, err
	}
	return q.prepare(tx)
}*/

func (q *Query_get) Fetch(tx *sql.Tx) error {
	var err error
	if q.rows, err = tx.QueryContext(q.ctx, q.sql, q.sql_values...); err != nil {
		return err
	}
	if q.res_cols, err = q.rows.Columns(); err != nil {
		return err
	}
	q.res_cols_num = len(q.res_cols)
	return nil
}

func (q *Query_get) Fetch_row(tx *sql.Tx) (Row_result, error) {
	if err := q.Fetch(tx); err != nil {
		return Row_result{}, err
	}
	if !q.Next() {
		return Row_result{}, q.Row_error()
		
		//err == sql.ErrNoRows
	}
	return q.row, nil
}

func (q *Query_get) Next() bool {
	if !q.rows.Next() {
		return false
	}
	
	cols 	:= make([]any, q.res_cols_num)
	ptrs 	:= make([]any, q.res_cols_num)
	for i, _ := range cols {
		ptrs[i] = &cols[i]
	}
	
	if err := q.rows.Scan(ptrs...); err != nil {
		q.row_error = err
		return false
	}
	
	/*
	int64
	float64
	bool
	[]byte
	string
	time.Time
	nil
	*/
	
	q.row = Row_result{}
	for i, name := range q.res_cols {
		value := *ptrs[i].(*any)
		if value == nil {
			q.row[name] = value
		}else{
			switch v := value.(type) {
			case []uint8:
				q.row[name] = string(v)
			case int64:
				q.row[name] = v
			default:
				panic(fmt.Sprintf("Invalid database type: %s %v (%T)", name, value, value))
			}
		}
	}
	
	return true
}

func (q *Query_get) Row() Row_result {
	return q.row
}

func (q *Query_get) Row_error() error {
	if err := q.rows.Err(); err != nil {
		return err
	}
	return q.row_error
}

func (q *Query_get) prepare_select() (Error_code, error) {
	//	Check if table is private
	if q.public && !q.view.Public() {
		return q.error_table_private()
	}
	
	q.init()
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
	
	q.compile_sql()
	
	return 0, nil
}

func (q *Query_get) compile_sql(){
	q.sql_values 	= []any{}
	q.sql 			= "SELECT "+q.sql_select_clause()+"\nFROM "+q.sql_from_clause()
	
	if q.joined {
		q.sql += q.sql_joins()
	}
	
	if len(q.out_where) != 0 {
		q.sql += "\nWHERE "+q.sql_where_clause()
	}
	
	if !q.read_count {
		if len(q.in_limit) != 0 {
			q.sql += "\nLIMIT "+int_list_string(q.in_limit)
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
	switch len(q.in_limit) {
	case 0:
	case 1:
		if q.in_limit[0] == 0 {
			q.error_limit_value()
		}
	case 2:
		if q.in_limit[1] == 0 {
			q.error_limit_value()
		}
	default:
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
	if len(q.joins) == 0 {
		return ""
	}
	return "\n"+strings.Join(q.joins, "\n")
}