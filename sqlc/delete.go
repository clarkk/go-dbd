package sqlc

type Delete struct {
	query_where
}

func NewDelete(table string) *Delete {
	return &Delete{
		query_where: query_where{
			query: query{
				table:		table,
				joins: 		[]join{},
				data:		[]any{},
			},
			where:		[]where_clause{},
			where_data:	[]any{},
		},
	}
}

/*func (q *Delete) Left_join(table, t, field, field_foreign string) *Delete {
	q.left_join(table, t, field, field_foreign)
	return q
}*/

func (q *Delete) Where(clauses *where) *Delete {
	clauses.compile(q)
	return q
}

func (q *Delete) Compile() (string, error){
	if err := q.compile_tables(); err != nil {
		return "", err
	}
	s := q.compile_delete()
	/*if len(q.joins) != 0 {
		s += q.compile_joins()
	}*/
	if len(q.where) != 0 {
		s += q.compile_where()
	}
	return s, nil
}

func (q *Delete) compile_delete() string {
	s := "DELETE FROM ."+q.table
	/*if q.joined {
		s += " "+q.t
	}*/
	return s+"\n"
}