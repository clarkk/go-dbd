package sqlc

import (
	"fmt"
	"strconv"
	"strings"
)

const (
	alloc_select_field		= 10
	alloc_field_assignment	= 10
	alloc_where_condition	= 15
	alloc_join_clause		= 40
	
	char_table = "abcdefghijklmnopqrstuvwxyz"
)

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
	
	query_where struct {
		query_join
		where 		[]where_clause
		where_data 	[]any
		or_groups	[]*or_group
		use_id		bool
		id 			uint64
	}
	
	or_group struct {
		where 		[]where_clause
		where_data 	[]any
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

func (q *query_where) where_clause(clause where_clause, value any){
	q.where 		= append(q.where, clause)
	q.where_data 	= append(q.where_data, value)
}

func (q *query_where) where_or_group() *or_group {
	g := &or_group{}
	q.or_groups = append(q.or_groups, g)
	return g
}

func (g *or_group) where_clause(clause where_clause, value any){
	g.where 		= append(g.where, clause)
	g.where_data 	= append(g.where_data, value)
}

func (q *query_where) compile_where() (string, error){
	length := len(q.where) + len(q.or_groups)
	if q.use_id {
		length++
	}
	if length == 0 {
		return "", nil
	}
	
	var sb strings.Builder
	//	Pre-allocation
	sb.Grow((len(q.or_groups) + len(q.where)) * alloc_where_condition)
	
	sb.WriteString("WHERE ")
	first := true
	
	if q.use_id {
		q.field(&sb, "id")
		sb.WriteByte('=')
		sb.WriteString(strconv.FormatUint(q.id, 10))
		first = false
	}
	
	//	Apply "or groups"
	if q.or_groups != nil {
		for _, group := range q.or_groups {
			if first {
				first = false
			} else {
				sb.WriteString(" && ")
			}
			sb.WriteByte('(')
			
			for i, clause := range group.where {
				if i > 0 {
					sb.WriteString(" || ")
				}
				q.field(&sb, clause.field)
				sb.WriteString(clause.sql)
				
				q.append_data(group.where_data[i])
			}
			
			sb.WriteByte(')')
		}
	}
	
	duplicates := map[string]string{}
	for i, clause := range q.where {
		if operator, ok := duplicates[clause.field]; ok {
			switch operator {
			//	Operator not compatable with "oposite" operators
			case op_null:
				if clause.operator == op_not_null {
					return "", where_operator_error(clause.field, operator, clause.operator)
				}
			case op_not_null:
				if clause.operator == op_null {
					return "", where_operator_error(clause.field, operator, clause.operator)
				}
			
			//	Operator not compatable with other operators
			case op_eq, op_not_eq, op_bt, op_not_bt, op_in, op_not_in:
				return "", where_operator_error(clause.field, operator, clause.operator)
			
			//	Operator only compatable with "oposite" operators
			case op_gt, op_gteq:
				if clause.operator != op_lt && clause.operator != op_lteq {
					return "", where_operator_error(clause.field, operator, clause.operator)
				}
			case op_lt, op_lteq:
				if clause.operator != op_gt && clause.operator != op_gteq {
					return "", where_operator_error(clause.field, operator, clause.operator)
				}
			}
		} else {
			duplicates[clause.field] = clause.operator
		}
		
		if first {
			first = false
		} else {
			sb.WriteString(" && ")
		}
		
		if clause.subquery != nil {
			sql, err := clause.subquery.Compile()
			if err != nil {
				return "", err
			}
			clause.sql = strings.Replace(clause.sql, "?", sql, 1)
		}
		
		q.field(&sb, clause.field)
		sb.WriteString(clause.sql)
		
		if clause.operator == op_null || clause.operator == op_not_null {
			continue
		}
		
		//	Apply data
		if clause.subquery != nil {
			q.data = append(q.data, clause.subquery.Data()...)
		} else {
			q.append_data(q.where_data[i])
		}
	}
	sb.WriteByte('\n')
	return sb.String(), nil
}

func (q *query) append_data(val any){
	//	Flatten data slices
	if v, ok := val.([]any); ok {
		q.data = append(q.data, v...)
	} else {
		q.data = append(q.data, val)
	}
}

func placeholder_value_array(count int, sb *strings.Builder){
	if count == 0 {
		return
	}
	sb.Grow((count * 2) - 1)
	sb.WriteByte('?')
	for i := 1; i < count; i++ {
		sb.WriteString(",?")
	}
}

func where_operator_error(field, operator1, operator2 string) error {
	return fmt.Errorf("Where clause operator incompatable on same field (%s): %s %s", field, operator1, operator2)
}