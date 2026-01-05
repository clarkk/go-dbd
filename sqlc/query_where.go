package sqlc

import (
	"fmt"
	
	//"github.com/clarkk/go-dbd/sqlc/sb_audit"
)

type query_where struct {
	query_join
	where_clause	*Where_clause
	use_id			bool
	id 				uint64
}

func (q *query_where) compile_where(sb *sbuilder) error {
	num, alloc, alloc_data := q.get_alloc()
	
	//audit := sb_audit.Base(sb, "where")
	
	if q.use_id {
		num++
		alloc += 4	//	"id=?"
		alloc_data++
	}
	
	if num == 0 {
		return nil
	}
	
	//	Pre-allocation
	alloc += 7 + num * 5	//	"WHERE \n" + " AND "
	if q.joined {
		alloc += alloc_data * 3
	}
	sb.Alloc(alloc)
	//audit.Grow(alloc)
	q.alloc_data_capacity(alloc_data + len(q.data))
	
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
	
	//audit.Audit()
	
	sb.WriteByte('\n')
	return nil
}

func (q *query_where) walk_where_clause(sb *sbuilder, clause *Where_clause, duplicates *map[string]string, first *bool) error {
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

func (q *query_where) get_alloc() (int, int, int){
	if q.where_clause == nil {
		return 0, 0, 0
	}
	
	num				:= q.where_clause.num
	alloc			:= q.where_clause.alloc
	alloc_data	:= q.where_clause.alloc_data
	if q.where_clause.wrapped != nil {
		num				+= q.where_clause.wrapped.num
		alloc			+= q.where_clause.wrapped.alloc
		alloc_data	+= q.where_clause.wrapped.alloc_data
	}
	for _, group := range q.where_clause.or_groups {
		num				+= group.num
		alloc			+= group.alloc
		alloc_data	+= group.alloc_data
	}
	return num, alloc, alloc_data
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