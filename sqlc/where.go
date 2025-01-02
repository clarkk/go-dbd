package sqlc

import "strings"

const (
	OP_BT 		= "bt"
	OP_NOT_BT 	= "not_bt"
	OP_IN 		= "in"
	OP_NOT_IN 	= "not_in"
)

type (
	Where_clauser interface {
		where_clause(where_clause, ...any)
		field(string) string
	}
	
	where struct {
		fields 		[]string
		operators 	[]string
		values 		[]any
	}
)

func Where() *where {
	return &where{}
}

func (w *where) Eq(field string, value any) *where {
	w.clause(field, "=", value)
	return w
}

func (w *where) Eqs(fields map[string]any) *where {
	for k, v := range fields {
		w.clause(k, "=", v)
	}
	return w
}

func (w *where) Gt(field string, value any) *where {
	w.clause(field, ">", value)
	return w
}

func (w *where) Gt_eq(field string, value any) *where {
	w.clause(field, ">=", value)
	return w
}

func (w *where) Lt(field string, value any) *where {
	w.clause(field, "<", value)
	return w
}

func (w *where) Lt_eq(field string, value any) *where {
	w.clause(field, "<=", value)
	return w
}

func (w *where) Bt(field string, value1, value2 any) *where {
	w.clause(field, OP_BT, []any{value1, value2})
	return w
}

func (w *where) Not_bt(field string, value1, value2 any) *where {
	w.clause(field, OP_NOT_BT, []any{value1, value2})
	return w
}

func (w *where) In(field string, values []any) *where {
	w.clause(field, OP_IN, values)
	return w
}

func (w *where) Not_in(field string, values []any) *where {
	w.clause(field, OP_NOT_IN, values)
	return w
}

func (w *where) compile(query Where_clauser){
	for i, field := range w.fields {
		switch w.operators[i] {
		case OP_BT:
			query.where_clause(
				where_clause{
					field:	field,
					sql:	"%s BETWEEN ? AND ?",
				},
				w.values[i],
			)
		case OP_NOT_BT:
			query.where_clause(
				where_clause{
					field:	field,
					sql:	"%s NOT BETWEEN ? AND ?",
				},
				w.values[i],
			)
		case OP_IN:
			query.where_clause(
				where_clause{
					field:	field,
					sql:	"%s IN ("+where_clause_in(len(w.values[i].([]any)))+")",
				},
				w.values[i],
			)
		case OP_NOT_IN:
			query.where_clause(
				where_clause{
					field:	field,
					sql:	"%s NOT IN ("+where_clause_in(len(w.values[i].([]any)))+")",
				},
				w.values[i],
			)
		default:
			query.where_clause(
				where_clause{
					field:	field,
					sql:	"%s"+w.operators[i]+"?",
				},
				w.values[i],
			)
		}
	}
}

func (w *where) clause(field, operator string, value any){
	w.fields 	= append(w.fields, field)
	w.operators = append(w.operators, operator)
	w.values 	= append(w.values, value)
}

func where_clause_in(i int) string {
	s := strings.Repeat("?,", i)
	return s[:len(s)-1]
}