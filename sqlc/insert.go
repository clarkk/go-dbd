package sqlc

import "strings"

type Insert struct {
	query
	fields 		map[string]any
}

func NewInsert(table string) *Insert {
	return &Insert{
		query: query{
			table:		table,
			joins: 		[]join{},
			data:		[]any{},
		},
		fields: 	map[string]any{},
	}
}

func (q *Insert) Fields(fields map[string]any) *Insert {
	q.fields = fields
	return q
}

/*func (q *Insert) Left_join(table, t, field, field_foreign string) *Insert {
	q.left_join(table, t, field, field_foreign)
	return q
}*/

func (q *Insert) Compile() (string, error){
	if err := q.compile_tables(); err != nil {
		return "", err
	}
	s := q.compile_insert()+q.compile_fields()
	/*if len(q.joins) != 0 {
		s += q.compile_joins()
	}*/
	return s, nil
}

func (q *Insert) compile_insert() string {
	s := "INSERT ."+q.table
	/*if q.joined {
		s += " "+q.t
	}*/
	return s+"\n"
}

func (q *Insert) compile_fields() string {
	list := make([]string, len(q.fields))
	i := 0
	for k, v := range q.fields {
		list[i]	= q.field(k)+"=?"
		q.data 	= append(q.data, v)
		i++
	}
	return "SET "+strings.Join(list, ", ")+"\n"
}