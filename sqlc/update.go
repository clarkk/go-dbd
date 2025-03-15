package sqlc

import "strings"

type Update_query struct {
	query_where
	fields 		Map
}

func Update_id(table string, id uint64) *Update_query {
	q := Update(table)
	q.use_id 	= true
	q.id 		= id
	return q
}

func Update(table string) *Update_query {
	return &Update_query{
		query_where: query_where{
			query_join: query_join{
				query: query{
					table:		table,
					data:		[]any{},
				},
				joins: 		[]join{},
			},
			where:		[]where_clause{},
			where_data:	[]any{},
		},
		fields: 	Map{},
	}
}

func (q *Update_query) Fields(fields map[string]any) *Update_query {
	q.fields = fields
	return q
}

/*func (q *Update_query) Left_join(table, t, field, field_foreign string) *Update_query {
	q.left_join(table, t, field, field_foreign)
	return q
}*/

func (q *Update_query) Where(clauses *Where_clause) *Update_query {
	clauses.apply(q)
	return q
}

func (q *Update_query) Compile() (string, error){
	if err := q.compile_tables(); err != nil {
		return "", err
	}
	s := q.compile_update()+q.compile_fields()
	/*if len(q.joins) != 0 {
		s += q.compile_joins()
	}*/
	sql_where, err := q.compile_where()
	if err != nil {
		return "", err
	}
	s += sql_where
	return s, nil
}

func (q *Update_query) compile_update() string {
	s := "UPDATE ."+q.table
	/*if q.joined {
		s += " "+q.t
	}*/
	return s+"\n"
}

func (q *Update_query) compile_fields() string {
	list := make([]string, len(q.fields))
	i := 0
	for k, v := range q.fields {
		list[i]	= q.field(k)+"=?"
		q.data 	= append(q.data, v)
		i++
	}
	return "SET "+strings.Join(list, ", ")+"\n"
}