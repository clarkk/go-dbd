package sqlc

import (
	"fmt"
	"sync"
	"strings"
)

const (
	alloc_select_field		= 10
	alloc_field_assignment	= 10
	alloc_where_condition	= 15
	alloc_join_clause		= 40
	
	char_table = "abcdefghijklmnopqrstuvwxyz"
)

var builder_pool = sync.Pool{
	New: func() any {
		return &strings.Builder{}
	},
}

type (
	SQL interface {
		Compile() (string, error)
		Data() []any
	}
	
	Map map[string]any
	
	query struct {
		table 	string
		data 	[]any
	}
	
	join struct {
		mode 			string
		table 			string
		t 				string
		join_t			string
		field 			string
		field_foreign 	string
		conditions		Map
	}
)

func SQL_debug(q SQL) string {
	s, _ := q.Compile()
	for _, value := range q.Data() {
		s = strings.Replace(s, "?", fmt.Sprintf("%v", value), 1)
	}
	return strings.TrimSpace(s)
}

func SQL_error(msg string, q SQL, err error) string {
	return msg+"\n"+err.Error()+"\n"+SQL_debug(q)
}

func (q *query) Data() []any {
	return q.data
}

func check_operator_compatibility(prev_operator, new_operator, field string) error {
	switch prev_operator {
	//	Operator not compatable with "oposite" operators
	case op_null:
		if new_operator == op_not_null {
			return where_operator_error(field, prev_operator, new_operator)
		}
	case op_not_null:
		if new_operator == op_null {
			return where_operator_error(field, prev_operator, new_operator)
		}
	
	//	Operator not compatable with other operators
	case op_eq, op_not_eq, op_bt, op_not_bt, op_in, op_not_in:
		return where_operator_error(field, prev_operator, new_operator)
	
	//	Operator only compatable with "oposite" operators
	case op_gt, op_gteq:
		if new_operator != op_lt && new_operator != op_lteq {
			return where_operator_error(field, prev_operator, new_operator)
		}
	case op_lt, op_lteq:
		if new_operator != op_gt && new_operator != op_gteq {
			return where_operator_error(field, prev_operator, new_operator)
		}
	}
	return nil
}

func (q *query) append_data(val any){
	if val == nil {
		return
	}
	
	//	Flatten data slices
	if v, ok := val.([]any); ok {
		length := len(v)
		if length == 0 {
			return
		}
		
		q.alloc_data_capacity(len(q.data) + length)
		
		q.data = append(q.data, v...)
	} else {
		q.data = append(q.data, val)
	}
}

func (q *query) alloc_data_capacity(total int){
	if cap(q.data) < total {
		new_data := make([]any, len(q.data), total)
		copy(new_data, q.data)
		q.data = new_data
	}
}

func placeholder_value_array(count int, sb *strings.Builder){
	if count == 0 {
		return
	}
	sb.WriteByte('?')
	for i := 1; i < count; i++ {
		sb.WriteString(",?")
	}
}

func placeholder_value_array_length(count int) int {
	if count == 0 {
		return 0
	}
	return (count * 2) - 1
}

func where_operator_error(field, operator1, operator2 string) error {
	return fmt.Errorf("Where clause operator incompatable on same field (%s): %s %s", field, operator1, operator2)
}