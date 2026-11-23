package sqlc

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