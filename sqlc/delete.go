package sqlc

type delete struct {
	query_where
	id 			int
}

func Delete(table string, id int) *delete {
	return &delete{
		query_where: query_where{
			query: query{
				table:		table,
				joins: 		[]join{},
				data:		[]any{},
			},
			where:		[]where_clause{},
			where_data:	[]any{},
		},
		id: id,
	}
}

/*func (q *delete) Left_join(table, t, field, field_foreign string) *delete {
	q.left_join(table, t, field, field_foreign)
	return q
}*/

func (q *delete) Where(clauses *where) *delete {
	clauses.compile(q)
	return q
}

func (q *delete) Compile() (string, error){
	if err := q.compile_tables(); err != nil {
		return "", err
	}
	s := q.compile_delete()
	/*if len(q.joins) != 0 {
		s += q.compile_joins()
	}*/
	s += q.compile_where(q.id)
	return s, nil
}

func (q *delete) compile_delete() string {
	s := "DELETE FROM ."+q.table
	/*if q.joined {
		s += " "+q.t
	}*/
	return s+"\n"
}