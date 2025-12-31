package sqlc

import "strings"

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
		wrapped		*Where_clause
		or_groups	[]*Where_clause
		conditions	[]condition
	}
	
	condition struct {
		field		string
		operator	string
		value		any
	}
	
	where_clauser interface {
		where_clause(clause where_clause, values any)
		where_or_group() *or_group
	}
	
	where_clause struct {
		field 		string
		operator 	string
		sql 		string
		subquery	*Select_query
	}
)

func Where() *Where_clause {
	return &Where_clause{}
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
	for k, v := range fields {
		w.clause(k, op_eq, v)
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

func (w *Where_clause) In_subquery(field string, query SQL) *Where_clause {
	w.clause(field, op_in_subquery, query)
	return w
}

func (w *Where_clause) Not_in(field string, values []any) *Where_clause {
	w.clause(field, op_not_in, values)
	return w
}

func (w *Where_clause) apply(query where_clauser){
	if w.wrapped != nil {
		w.wrapped.apply(query)
	}
	
	if w.or_groups != nil {
		for _, group := range w.or_groups {
			group.apply_or_group(query)
		}
	}
	
	for _, field := range w.conditions {
		switch field.operator {
		case op_null, op_not_null:
			sql := " IS NULL"
			if field.operator == op_not_null {
				sql = " IS NOT NULL"
			}
			query.where_clause(
				where_clause{
					field:		field.field,
					operator:	field.operator,
					sql:		sql,
				},
				field.value,
			)
			
		case op_bt, op_not_bt:
			sql := " "+sql_op_bt
			if field.operator == op_not_bt {
				sql = " NOT"+sql
			}
			query.where_clause(
				where_clause{
					field:		field.field,
					operator:	field.operator,
					sql:		sql,
				},
				field.value,
			)
			
		case op_in, op_not_in:
			var sb strings.Builder
			if field.operator == op_not_in {
				sb.WriteString(" NOT")
			}
			sb.WriteString(" IN (")
			where_clause_in(len(field.value.([]any)), &sb)
			sb.WriteByte(')')
			
			query.where_clause(
				where_clause{
					field:		field.field,
					operator:	field.operator,
					sql:		sb.String(),
				},
				field.value,
			)
			
		case op_in_subquery:
			query.where_clause(
				where_clause{
					field:		field.field,
					operator:	field.operator,
					sql:		" IN (?)",
					subquery:	field.value.(*Select_query),
				},
				nil,
			)
		
		default:
			query.where_clause(
				where_clause{
					field:		field.field,
					operator:	field.operator,
					sql:		field.operator+"?",
				},
				field.value,
			)
		}
	}
}

func (w *Where_clause) apply_or_group(query where_clauser){
	group := query.where_or_group()
	for _, field := range w.conditions {
		switch field.operator {
		case op_bt:
			group.where_clause(
				where_clause{
					field:		field.field,
					operator:	field.operator,
					sql:		" "+sql_op_bt,
				},
				field.value,
			)
			
		default:
			group.where_clause(
				where_clause{
					field:		field.field,
					operator:	field.operator,
					sql:		field.operator+"?",
				},
				field.value,
			)
		}
	}
}

func (w *Where_clause) clause(field, operator string, value any){
	w.conditions = append(w.conditions, condition{
		field:		field,
		operator:	operator,
		value:		value,
	})
}

func where_clause_in(count int, sb *strings.Builder){
	sb.Grow((count * 2) - 1)
	for i := range count {
		if i > 0 {
			sb.WriteByte(',')
		}
		sb.WriteByte('?')
	}
}