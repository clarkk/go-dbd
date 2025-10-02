package sqlc

const op_update_add = "+"

type Fields_clause struct {
	fields 		[]string
	operators 	[]string
	values 		[]any
}

func Fields() *Fields_clause {
	return &Fields_clause{}
}

func (f *Fields_clause) Value(field string, value any) *Fields_clause {
	f.clause(field, "", value)
	return f
}

func (f *Fields_clause) Add(field string, value any) *Fields_clause {
	f.clause(field, op_update_add, value)
	return f
}

func (f *Fields_clause) clause(field, operator string, value any){
	f.fields 	= append(f.fields, field)
	f.operators = append(f.operators, operator)
	f.values 	= append(f.values, value)
}