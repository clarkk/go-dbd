package sqlc

import "fmt"

type where_writer struct {
	q			*query_where
	c			*compiler
	or_group	bool
	has_term	bool
	duplicates	map[string]Operator
}

func (w *where_writer) write_clause(clause *Where_clause) error {
	if clause.wrapped != nil {
		if err := w.write_wrapped(clause.wrapped); err != nil {
			return err
		}
	}
	for i := range clause.conditions {
		if err := w.write_condition(&clause.conditions[i]); err != nil {
			return err
		}
	}
	for _, group := range clause.or_groups {
		if err := w.write_group(group, true); err != nil {
			return err
		}
	}
	return nil
}

func (w *where_writer) write_wrapped(clause *Where_clause) error {
	if w.or_group {
		return w.write_group(clause, false)
	}
	return w.write_clause(clause)
}

func (w *where_writer) write_group(clause *Where_clause, or_group bool) error {
	if clause == nil {
		return fmt.Errorf("nil WHERE group")
	}
	
	w.begin_term()
	w.c.sb.WriteByte('(')
	
	child := where_writer{
		q:			w.q,
		c:			w.c,
		or_group:	or_group,
	}
	if err := child.write_clause(clause); err != nil {
		return err
	}
	
	if !child.has_term {
		return fmt.Errorf("Empty WHERE group")
	}
	
	w.c.sb.WriteByte(')')
	return nil
}

func (w *where_writer) write_condition(condition *where_condition) error {
	if !w.or_group {
		if err := w.check_duplicate(condition); err != nil {
			return err
		}
	}
	
	w.begin_term()
	return w.q.write_condition_data(w.c, condition)
}

func (w *where_writer) begin_term(){
	if w.has_term {
		if w.or_group {
			w.c.sb.WriteString(" OR ")
		} else {
			w.c.sb.WriteString(" AND ")
		}
	}
	w.has_term = true
}

func (w *where_writer) check_duplicate(condition *where_condition) error {
	if previous, ok := w.duplicates[condition.field]; ok {
		return check_operator_compatibility(previous, condition.operator, condition.field)
	}
	
	if w.duplicates == nil {
		w.duplicates = make(map[string]Operator, 2)
	}
	w.duplicates[condition.field] = condition.operator
	return nil
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
	return fmt.Errorf("Incompatible WHERE conditions on field %q", field)
}