package sqlc

import "strings"

type Update struct {
	query_where
	fields 		map[string]any
}

func NewUpdate(table string) *Update {
	return &Update{
		query_where: query_where{
			query: query{
				table:		table,
				joins: 		[]join{},
				data:		[]any{},
			},
			where:		[]where_clause{},
			where_data:	[]any{},
		},
		fields: 	map[string]any{},
	}
}

func (q *Update) Fields(fields map[string]any) *Update {
	q.fields = fields
	return q
}

/*func (q *Update) Left_join(table, t, field, field_foreign string) *Update {
	q.left_join(table, t, field, field_foreign)
	return q
}*/

func (q *Update) Where(clauses *where) *Update {
	clauses.compile(q)
	return q
}

func (q *Update) Compile() (string, error){
	if err := q.compile_tables(); err != nil {
		return "", err
	}
	s := q.compile_update()+q.compile_fields()
	/*if len(q.joins) != 0 {
		s += q.compile_joins()
	}*/
	if len(q.where) != 0 {
		s += q.compile_where()
	}
	return s, nil
}

func (q *Update) compile_update() string {
	s := "UPDATE ."+q.table
	/*if q.joined {
		s += " "+q.t
	}*/
	return s+"\n"
}

func (q *Update) compile_fields() string {
	list := make([]string, len(q.fields))
	i := 0
	for k, v := range q.fields {
		list[i]	= q.field(k)+"=?"
		q.data 	= append(q.data, v)
		i++
	}
	return "SET "+strings.Join(list, ", ")+"\n"
}