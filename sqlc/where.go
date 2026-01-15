package sqlc

import "slices"

const (
	op_eq 			= "="
	op_not_eq 		= "!="
	op_gt 			= ">"
	op_gteq 		= ">="
	op_lt 			= "<"
	op_lteq 		= "<="
	op_null 		= "null"
	op_not_null 	= "not_null"
	op_bt 			= "bt"
	op_not_bt 		= "not_bt"
	op_in 			= "in"
	op_not_in 		= "not_in"
	
	op_in_subquery 	= "in_sub"
	
	sql_op_bt		= "BETWEEN ? AND ?"
)

type (
	Where_clause struct {
		wrapped			*Where_clause
		or_groups		[]*Where_clause
		conditions		[]where_condition
		
		num				int
		alloc			int
		alloc_data		int
	}
	
	where_condition struct {
		field			string
		operator		string
		value			any
	}
)

func Where() *Where_clause {
	return &Where_clause{
		//	Pre-allocation with 2 conditions
		conditions: make([]where_condition, 0, 2),
	}
}

func (w *Where_clause) Wrap(wrap *Where_clause) *Where_clause {
	w.wrapped = wrap
	return w
}

func (w *Where_clause) Or_group(where *Where_clause) *Where_clause {
	w.or_groups = append(w.or_groups, where)
	return w
}

func (w *Where_clause) Eq(field string, value any) *Where_clause {
	w.clause(field, op_eq, value)
	return w
}

func (w *Where_clause) Not_eq(field string, value any) *Where_clause {
	w.clause(field, op_not_eq, value)
	return w
}

func (w *Where_clause) Eqs(fields map[string]any) *Where_clause {
	//	Sort keys
	keys := make([]string, len(fields))
	var i int
	for k := range fields {
		keys[i] = k
		i++
	}
	slices.Sort(keys)
	for _, k := range keys {
		w.clause(k, op_eq, fields[k])
	}
	return w
}

func (w *Where_clause) Gt(field string, value any) *Where_clause {
	w.clause(field, op_gt, value)
	return w
}

func (w *Where_clause) Gt_eq(field string, value any) *Where_clause {
	w.clause(field, op_gteq, value)
	return w
}

func (w *Where_clause) Lt(field string, value any) *Where_clause {
	w.clause(field, op_lt, value)
	return w
}

func (w *Where_clause) Lt_eq(field string, value any) *Where_clause {
	w.clause(field, op_lteq, value)
	return w
}

func (w *Where_clause) Null(field string) *Where_clause {
	w.clause(field, op_null, nil)
	return w
}

func (w *Where_clause) Not_null(field string) *Where_clause {
	w.clause(field, op_not_null, nil)
	return w
}

func (w *Where_clause) Bt(field string, value1, value2 any) *Where_clause {
	w.clause(field, op_bt, []any{value1, value2})
	return w
}

func (w *Where_clause) Not_bt(field string, value1, value2 any) *Where_clause {
	w.clause(field, op_not_bt, []any{value1, value2})
	return w
}

func (w *Where_clause) In(field string, values []any) *Where_clause {
	w.clause(field, op_in, values)
	return w
}

func (w *Where_clause) Not_in(field string, values []any) *Where_clause {
	w.clause(field, op_not_in, values)
	return w
}

func (w *Where_clause) In_subquery(field string, query *Select_query) *Where_clause {
	w.clause(field, op_in_subquery, query)
	return w
}

func (w *Where_clause) write_condition(sb *sbuilder, field *where_condition) (*Select_query, error){
	var subquery *Select_query
	
	switch field.operator {
	case op_null:
		sb.WriteString(" IS NULL")
		
	case op_not_null:
		sb.WriteString(" IS NOT NULL")
		
	case op_bt, op_not_bt:
		if field.operator == op_not_bt {
			sb.WriteString(" NOT")
		}
		sb.WriteByte(' ')
		sb.WriteString(sql_op_bt)
		
	case op_in, op_not_in:
		if field.operator == op_not_in {
			sb.WriteString(" NOT")
		}
		sb.WriteString(" IN (")
		field_placeholder_list(len(field.value.([]any)), sb)
		sb.WriteByte(')')
		
	case op_in_subquery:
		subquery = field.value.(*Select_query)
		sql_subquery, err := subquery.Compile()
		if err != nil {
			return nil, err
		}
		sb.WriteString(" IN (\n")
		sb.WriteString(sql_subquery)
		sb.WriteByte(')')
		
	default:
		sb.WriteString(field.operator)
		sb.WriteByte('?')
	}
	
	return subquery, nil
}

func (w *Where_clause) clause(field, operator string, value any){
	var (
		alloc		int
		alloc_data	int
	)
	switch operator {
	case op_null:
		alloc			= 8
		
	case op_not_null:
		alloc			= 12
		
	case op_bt, op_not_bt:
		alloc_data		= 2
		alloc			= 1 + len(sql_op_bt)
		if operator == op_not_bt {
			alloc		+= 4
		}
		
	case op_in, op_not_in:
		alloc_data		= len(value.([]any))
		alloc			= 6 + alloc_field_placeholder_list(alloc_data)
		if operator == op_not_in {
			alloc		+= 4
		}
		
	case op_in_subquery:
		alloc			= 7 + alloc_query
		
	default:
		alloc_data		= 1
		alloc			= 1 + len(operator)
	}
	
	w.conditions = append(w.conditions, where_condition{
		field:		field,
		operator:	operator,
		value:		value,
	})
	w.num++
	w.alloc			+= alloc + len(field)
	w.alloc_data	+= alloc_data
}

func (w *Where_clause) get_alloc() (num, alloc, alloc_data int){
	num			= w.num
	alloc		= w.alloc
	alloc_data	= w.alloc_data
	if w.wrapped != nil {
		n, a, ad := w.wrapped.get_alloc()
		num			+= n
		alloc		+= a
		alloc_data	+= ad
	}
	for _, group := range w.or_groups {
		if group != nil {
			n, a, ad := group.get_alloc()
			num			+= n
			alloc		+= a
			alloc_data	+= ad
		}
	}
	return num, alloc, alloc_data
}

func (w *Where_clause) collect_aliases(list alias_collect){
	if w == nil {
		return
	}
	if w.wrapped != nil {
		w.wrapped.collect_aliases(list)
	}
	for _, group := range w.or_groups {
		if group != nil {
			group.collect_aliases(list)
		}
	}
	for _, f := range w.conditions {
		list.apply(f.field)
	}
}