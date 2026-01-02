package sqlc

const op_update_add = "+"

type (
	Fields_clause struct {
		entries		[]field_entry
	}
	
	field_entry struct {
		field		string
		operator	string
		value		any
	}
)

func Fields() *Fields_clause {
	return &Fields_clause{
		//	Pre-allocate 4 fields
		entries: make([]field_entry, 0, 4),
	}
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
	f.entries = append(f.entries, field_entry{
		field:		field,
		operator:	operator,
		value:		value,
	})
}