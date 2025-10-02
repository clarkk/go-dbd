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
	op_in_subquery 	= "in_sub"
	op_not_in 		= "not_in"
)

type (
	Where_clause struct {
		wrapped		*Where_clause
		fields 		[]string
		operators 	[]string
		values 		[]any
	}
	
	where_clauser interface {
		where_clause(clause where_clause, values ...any)
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

func (w *Where_clause) Wrap(wrap *Where_clause){
	w.wrapped = wrap
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
	
	for i, field := range w.fields {
		switch operator := w.operators[i]; operator {
		case op_null:
			query.where_clause(
				where_clause{
					field:		field,
					operator:	operator,
					sql:		" IS NULL",
				},
				w.values[i],
			)
		case op_not_null:
			query.where_clause(
				where_clause{
					field:		field,
					operator:	operator,
					sql:		" IS NOT NULL",
				},
				w.values[i],
			)
		case op_bt:
			query.where_clause(
				where_clause{
					field:		field,
					operator:	operator,
					sql:		" BETWEEN ? AND ?",
				},
				w.values[i],
			)
		case op_not_bt:
			query.where_clause(
				where_clause{
					field:		field,
					operator:	operator,
					sql:		" NOT BETWEEN ? AND ?",
				},
				w.values[i],
			)
		case op_in:
			query.where_clause(
				where_clause{
					field:		field,
					operator:	operator,
					sql:		" IN ("+where_clause_in(len(w.values[i].([]any)))+")",
				},
				w.values[i],
			)
		case op_in_subquery:
			query.where_clause(
				where_clause{
					field:		field,
					operator:	operator,
					sql:		" IN (?)",
					subquery:	w.values[i].(*Select_query),
				},
				nil,
			)
		case op_not_in:
			query.where_clause(
				where_clause{
					field:		field,
					operator:	operator,
					sql:		" NOT IN ("+where_clause_in(len(w.values[i].([]any)))+")",
				},
				w.values[i],
			)
		default:
			query.where_clause(
				where_clause{
					field:		field,
					operator:	operator,
					sql:		w.operators[i]+"?",
				},
				w.values[i],
			)
		}
	}
}

func (w *Where_clause) clause(field, operator string, value any){
	w.fields 	= append(w.fields, field)
	w.operators = append(w.operators, operator)
	w.values 	= append(w.values, value)
}

func where_clause_in(i int) string {
	s := strings.Repeat("?,", i)
	return s[:len(s)-1]
}