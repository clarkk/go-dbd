package sqlc

import "fmt"

type query_where struct {
	query_join
	where_clause	*Where_clause
	use_id			bool
	id 				uint64
}

func (q *query_where) compile_where(c *compiler, inner_condition func(c *compiler, first *bool)) error {
	num, alloc, alloc_data := q.get_alloc()
	
	if q.use_id {
		num++
		alloc += 4	//	"id=?"
		alloc_data++
	}
	
	if inner_condition != nil {
		num++
	}
	
	if num == 0 {
		return nil
	}
	
	//audit := Audit(sb, "where")
	
	//	Pre-allocation
	alloc += 7 + num * 5	//	"WHERE \n" + " AND "
	if c.use_alias {
		alloc += num * 3
	}
	
	c.sb.Alloc(alloc)
	//audit.Grow(alloc)
	c.alloc_data_capacity(alloc_data + len(c.data))
	
	c.sb.WriteString("WHERE ")
	first := true
	
	if q.use_id {
		c.write_field(q.t, "id")
		c.sb.WriteString("=?")
		c.append_data(q.id)
		first = false
	}
	
	if inner_condition != nil {
		inner_condition(c, &first)
	}
	
	if q.where_clause != nil {
		var duplicates map[string]Operator
		//	Only allocate if at least 2 conditions
		if len(q.where_clause.conditions) > 1 {
			//	Pre-allocation
			duplicates = make(map[string]Operator, 2)
		}
		
		if err := q.walk_where_clause(c, q.where_clause, &duplicates, &first); err != nil {
			return err
		}
	}
	c.sb.WriteByte('\n')
	//audit.Audit()
	return nil
}

func (q *query_where) walk_where_clause(c *compiler, clause *Where_clause, duplicates *map[string]Operator, first *bool) error {
	//	Apply wrapped conditions
	if clause.wrapped != nil {
		if err := q.walk_where_clause(c, clause.wrapped, duplicates, first); err != nil {
			return err
		}
	}
	
	//	Apply conditions
	for i := range clause.conditions {
		condition := &clause.conditions[i]	//	Avoid copying data
		
		if *duplicates != nil {
			if operator, ok := (*duplicates)[condition.field]; ok {
				if err := check_operator_compatibility(operator, condition.operator, condition.field); err != nil {
					return err
				}
			} else {
				(*duplicates)[condition.field] = condition.operator
			}
		} else {
			*duplicates = make(map[string]Operator, 2)
			(*duplicates)[condition.field] = condition.operator
		}
		
		if *first {
			*first = false
		} else {
			c.sb.WriteString(" AND ")
		}
		
		if err := q.write_condition_data(c, condition); err != nil {
			return err
		}
	}
	
	//	Apply "or groups"
	if clause.or_groups != nil {
		for _, group := range clause.or_groups {
			if *first {
				*first = false
			} else {
				c.sb.WriteString(" AND ")
			}
			
			c.sb.WriteByte('(')
			for i := range group.conditions {
				condition := &group.conditions[i]	//	Avoid copying data
				
				if i > 0 {
					c.sb.WriteString(" OR ")
				}
				
				if err := q.write_condition_data(c, condition); err != nil {
					return err
				}
			}
			c.sb.WriteByte(')')
		}
	}
	
	return nil
}

func (q *query_where) write_condition_data(c *compiler, condition *where_condition) error {
	c.write_field(q.t, condition.field)
	sub_data, err := write_operator_condition(&c.sb, condition.operator, condition.value)
	if err != nil {
		return err
	}
	
	//	Apply data
	switch condition.operator {
	case Op_null, Op_not_null:
	case Op_in_subquery:
		c.append_data(sub_data)
	default:
		c.append_data(condition.value)
	}
	return nil
}

func (q *query_where) get_alloc() (int, int, int){
	if q.where_clause == nil {
		return 0, 0, 0
	}
	return q.where_clause.get_alloc()
}

func check_operator_compatibility(current_operator, new_operator Operator, field string) error {
	if current_operator == new_operator {
		return where_operator_error(field, current_operator, new_operator)
	}
	
	switch current_operator {
	//	Operator not compatable with "oposite" operators
	case Op_null:
		if new_operator == Op_not_null {
			return where_operator_error(field, current_operator, new_operator)
		}
	case Op_not_null:
		if new_operator == Op_null {
			return where_operator_error(field, current_operator, new_operator)
		}
	
	//	Operator not compatable with other operators
	case Op_eq, Op_not_eq, Op_bt, Op_not_bt, Op_in, Op_not_in:
		return where_operator_error(field, current_operator, new_operator)
	
	//	Operator only compatable with "oposite" operators
	case Op_gt, Op_gteq:
		if new_operator != Op_lt && new_operator != Op_lteq {
			return where_operator_error(field, current_operator, new_operator)
		}
	case Op_lt, Op_lteq:
		if new_operator != Op_gt && new_operator != Op_gteq {
			return where_operator_error(field, current_operator, new_operator)
		}
	}
	return nil
}

func where_operator_error(field string, current_operator, new_operator Operator) error {
	if int(current_operator) < len(sql_ops) && int(new_operator) < len(sql_ops) {
		return fmt.Errorf("Where clause operator incompatable on same field (%s): %s %s", field, sql_ops[current_operator], sql_ops[new_operator])
	}
	return fmt.Errorf("incompatible WHERE conditions on field %q", field)
}