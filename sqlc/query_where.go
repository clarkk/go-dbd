package sqlc

import (
	"fmt"
	"strings"
)

type query_where struct {
	query_join
	where_clause	*Where_clause
	use_id			bool
	id 				uint64
}

func (q *query_where) compile_where(sb *strings.Builder) error {
	length := q.count_conditions()
	
	if q.use_id {
		length++
	}
	if length == 0 {
		return nil
	}
	
	//	Pre-allocation
	sb.Grow(7 + length * alloc_where_condition)
	q.alloc_data_capacity(length + len(q.data))
	
	sb.WriteString("WHERE ")
	first := true
	
	if q.use_id {
		q.write_field(sb, "id")
		sb.WriteString("=?")
		q.data = append(q.data, q.id)
		first = false
	}
	
	if q.where_clause != nil {
		var duplicates map[string]string
		//	Only allocate if at least 2 conditions
		if len(q.where_clause.conditions) > 1 {
			//	Pre-allocation
			duplicates = make(map[string]string, 2)
		}
		
		if err := q.walk_where_clause(sb, q.where_clause, &duplicates, &first); err != nil {
			return err
		}
	}
	
	sb.WriteByte('\n')
	return nil
}

func (q *query_where) walk_where_clause(sb *strings.Builder, clause *Where_clause, duplicates *map[string]string, first *bool) error {
	//	Apply wrapped conditions
	if clause.wrapped != nil {
		if err := q.walk_where_clause(sb, clause.wrapped, duplicates, first); err != nil {
			return err
		}
	}
	
	//	Apply conditions
	for _, condition := range clause.conditions {
		if *duplicates != nil {
			if operator, ok := (*duplicates)[condition.field]; ok {
				if err := check_operator_compatibility(operator, condition.operator, condition.field); err != nil {
					return err
				}
			} else {
				(*duplicates)[condition.field] = condition.operator
			}
		}
		
		if *first {
			*first = false
		} else {
			sb.WriteString(" AND ")
		}
		
		q.write_field(sb, condition.field)
		subquery, err := clause.write_condition(sb, condition)
		if err != nil {
			return err
		}
		
		if condition.operator == op_null || condition.operator == op_not_null {
			continue
		}
		
		//	Apply data
		if subquery != nil {
			q.append_data(subquery.Data())
		} else {
			q.append_data(condition.value)
		}
	}
	
	//	Apply "or groups"
	if clause.or_groups != nil {
		for _, group := range clause.or_groups {
			if *first {
				*first = false
			} else {
				sb.WriteString(" AND ")
			}
			
			sb.WriteByte('(')
			for i, condition := range group.conditions {
				if i > 0 {
					sb.WriteString(" OR ")
				}
				
				q.write_field(sb, condition.field)
				_, err := clause.write_condition(sb, condition)
				if err != nil {
					return err
				}
				
				q.append_data(condition.value)
			}
			sb.WriteByte(')')
		}
	}
	
	return nil
}

func (q *query_where) count_conditions() int {
	if q.where_clause == nil {
		return 0
	}
	n := len(q.where_clause.conditions)
	if q.where_clause.wrapped != nil {
		n += len(q.where_clause.wrapped.conditions)
	}
	for _, group := range q.where_clause.or_groups {
		n += len(group.conditions)
	}
	return n
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

func where_operator_error(field, prev_operator, new_operator string) error {
	return fmt.Errorf("Where clause operator incompatable on same field (%s): %s %s", field, prev_operator, new_operator)
}