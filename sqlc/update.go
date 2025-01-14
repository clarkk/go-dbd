package sqlc

import "strings"

type update struct {
	query_where
	fields 		map[string]any
}

func Update(table string, id int) *update {
	return &update{
		query_where: query_where{
			query: query{
				table:		table,
				joins: 		[]join{},
				data:		[]any{},
			},
			where:		[]where_clause{},
			where_data:	[]any{},
			id:			id,
		},
		fields: 	map[string]any{},
	}
}

func (q *update) Fields(fields map[string]any) *update {
	q.fields = fields
	return q
}

/*func (q *update) Left_join(table, t, field, field_foreign string) *update {
	q.left_join(table, t, field, field_foreign)
	return q
}*/

func (q *update) Where(clauses *where) *update {
	clauses.compile(q)
	return q
}

func (q *update) Compile() (string, error){
	if err := q.compile_tables(); err != nil {
		return "", err
	}
	s := q.compile_update()+q.compile_fields()
	/*if len(q.joins) != 0 {
		s += q.compile_joins()
	}*/
	s += q.compile_where()
	return s, nil
}

func (q *update) compile_update() string {
	s := "UPDATE ."+q.table
	/*if q.joined {
		s += " "+q.t
	}*/
	return s+"\n"
}

func (q *update) compile_fields() string {
	list := make([]string, len(q.fields))
	i := 0
	for k, v := range q.fields {
		list[i]	= q.field(k)+"=?"
		q.data 	= append(q.data, v)
		i++
	}
	return "SET "+strings.Join(list, ", ")+"\n"
}