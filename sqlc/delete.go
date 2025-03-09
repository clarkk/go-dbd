package sqlc

type Delete_query struct {
	query_where
}

func Delete(table string, id uint64) *Delete_query {
	return &Delete_query{
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
			id:			id,
		},
	}
}

/*func (q *Delete_query) Left_join(table, t, field, field_foreign string) *Delete_query {
	q.left_join(table, t, field, field_foreign)
	return q
}*/

func (q *Delete_query) Where(clauses *Where_clause) *Delete_query {
	clauses.apply(q)
	return q
}

func (q *Delete_query) Compile() (string, error){
	if err := q.compile_tables(); err != nil {
		return "", err
	}
	s := q.compile_delete()
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

func (q *Delete_query) compile_delete() string {
	s := "DELETE FROM ."+q.table
	/*if q.joined {
		s += " "+q.t
	}*/
	return s+"\n"
}